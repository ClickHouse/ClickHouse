#!/usr/bin/env python3
# Tags: no-fasttest

import logging
import os
from clickhouse_driver import Client


# Suppress https://github.com/regebro/tzlocal/blob/79da66645f8ce15c1b2d84a5cf350775c2ff81b3/tzlocal/unix.py#L142
class SuppressTimezoneWarning(logging.Filter):
    def filter(self, record):
        return "/etc/timezone is deprecated on Debian" not in record.getMessage()


logger = logging.getLogger("tzlocal")
logger.addFilter(SuppressTimezoneWarning())


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
