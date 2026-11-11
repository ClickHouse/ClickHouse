#!/usr/bin/env python3
# Tags: no-fasttest
# Regression test: IcebergIterator used to report file sizes to the progress
# callback before ObjectIteratorWithPathAndFileFilter could drop them,
# inflating total_bytes_to_read when a _file / _path predicate was present.
# Uses the native TCP protocol to capture total_bytes_to_read which is not
# exposed in HTTP progress JSON.

import os
import shutil

from clickhouse_driver import Client

def main():
    client = Client(os.environ.get("CLICKHOUSE_HOST", "localhost"),
                    port=int(os.environ.get("CLICKHOUSE_PORT_TCP", 9000)))

    db = os.environ.get("CLICKHOUSE_DATABASE", "default")
    table = f"t_iceberg_progress_{os.getpid()}"
    table_path = os.path.join(
        os.environ.get("CLICKHOUSE_USER_FILES", "/var/lib/clickhouse/user_files"),
        table,
    )

    try:
        client.execute("SET allow_experimental_insert_into_iceberg = 1")
        client.execute(f"""
            CREATE TABLE {db}.{table} (a Int32, b String)
            ENGINE = IcebergLocal('{table_path}/')
            PARTITION BY (a)
        """)
        client.execute(f"INSERT INTO {db}.{table} VALUES (1, repeat('x', 10000))")
        client.execute(f"INSERT INTO {db}.{table} VALUES (2, repeat('y', 10000))")
        client.execute(f"INSERT INTO {db}.{table} VALUES (3, repeat('z', 10000))")

        one_file = client.execute(f"SELECT _file FROM {db}.{table} LIMIT 1")[0][0]
        one_path = client.execute(f"SELECT _path FROM {db}.{table} LIMIT 1")[0][0]

        def get_total_bytes(query):
            gen = client.execute_with_progress(query)
            for _ in gen:
                pass
            gen.get_result()
            return gen.progress_totals.total_bytes

        total_all  = get_total_bytes(f"SELECT * FROM {db}.{table} FORMAT Null")
        total_file = get_total_bytes(f"SELECT * FROM {db}.{table} WHERE _file = '{one_file}' FORMAT Null")
        total_path = get_total_bytes(f"SELECT * FROM {db}.{table} WHERE _path = '{one_path}' FORMAT Null")

        def check(label, filtered, total):
            if total == 0 or filtered == 0:
                print(f"FAIL ({label}): could not capture progress "
                      f"(filtered={filtered}, total={total})")
            elif filtered * 100 // total < 60:
                print("OK")
            else:
                print(f"FAIL ({label}): filtered total_bytes_to_read ({filtered}) "
                      f"is not significantly less than full scan ({total})")

        check("_file", total_file, total_all)
        check("_path", total_path, total_all)

    finally:
        client.execute(f"DROP TABLE IF EXISTS {db}.{table}")
        shutil.rmtree(table_path, ignore_errors=True)


if __name__ == "__main__":
    main()
