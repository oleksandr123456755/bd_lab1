import psycopg2
import threading
import time

db_params = {
    "host": "localhost",
    "database": "db1",
    "user": "postgres",
    "password": "5432",
    "port": "5432",
}

def clear():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("update user_counter set counter = 0 where user_id = 1")
    cursor.execute("update user_counter set version = 0 where user_id = 1")
    conn.commit()
    conn.close()



def create_if_not_exists():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("""
DROP TABLE IF EXISTS user_counter;
CREATE TABLE IF NOT EXISTS user_counter
(
    user_id integer PRIMARY KEY,
    counter integer NOT NULL,
    version integer
);
INSERT INTO user_counter VALUES (1, 0, 0);""")
    conn.commit()
    conn.close()


def lost_update():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    conn.commit()
    for i in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
        conn.commit()

    conn.close()

def inplace_update():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    conn.commit()
    for i in range(10000):
        cursor.execute("update user_counter set counter = counter + 1 where user_id = %s",(1,))
        conn.commit()

    conn.close()

def row_level_locking():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    conn.commit()
    for i in range(10000):
        cursor.execute(("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE"))
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute("update user_counter set counter = %s where user_id = %s", (counter, 1))
        conn.commit()

    conn.close()

def optimistic_concurrency_control():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    conn.commit()
    for i in range(10000):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            counter, version = cursor.fetchone()
            counter = counter + 1
            cursor.execute("update user_counter set counter = %s, version = %s where user_id = %s and version = %s", (counter, version + 1, 1, version))
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                break

    conn.close()

def get_counter():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    final_counter = cursor.fetchone()[0]
    print(f"Counter is {final_counter}")
    conn.close()

if __name__ == '__main__':
    create_if_not_exists()

    start_time = time.time()
    for n, f in [('lost_update', lost_update), ('inplace_update', inplace_update), ('row_level_locking', row_level_locking), ('optimistic_concurrency_control', optimistic_concurrency_control)]:
        clear()
        threads = []
        for i in range(10):

            thread = threading.Thread(target=f)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        print(f'{n} -', round(time.time() - start_time, 3), 'seconds')
        start_time = time.time()
        get_counter()

