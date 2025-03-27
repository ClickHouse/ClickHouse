#!/usr/bin/env python3
# Tags: no-fasttest

import os

from clickhouse_driver import Client


def run(database):
    client = Client("localhost", user="default", password="")
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {database}.test (x Int32) ENGINE = Memory"
    )

    client.execute(f"INSERT INTO {database}.test (x) VALUES", [{"x": 100}])
    client.execute(
        f"INSERT INTO {database}.test (x) SETTINGS async_insert=1, wait_for_async_insert=1 VALUES",
        [{"x": 101}],
    )

    result = client.execute(f"SELECT * FROM {database}.test ORDER BY ALL")
    print(result)


if __name__ == "__main__":
    database = os.environ["CLICKHOUSE_DATABASE"]
    run(database)
