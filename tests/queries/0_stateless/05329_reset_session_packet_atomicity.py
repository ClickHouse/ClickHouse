#!/usr/bin/env python3

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "helpers"))
from native_session_client import CLICKHOUSE_DATABASE, NativeSessionClient, expect_error

DATABASE = f"{CLICKHOUSE_DATABASE}_reset_failure"
USER = f"reset_failure_{CLICKHOUSE_DATABASE}"


def dirty(client):
    client.query("SET max_block_size = 12345")
    client.query("SET param_kept = 'kept'")
    client.query("CREATE TEMPORARY TABLE reset_kept (x String)")
    client.query("INSERT INTO reset_kept VALUES ('kept')")


def check_retained(client, database):
    assert client.query(
        "SELECT currentDatabase(), toString(getSetting('max_block_size')), {kept:String}"
    ) == [[database, "12345", "kept"]]
    assert client.value("SELECT x FROM reset_kept") == "kept"


with NativeSessionClient() as admin:
    admin.query(f"DROP USER IF EXISTS {USER}")
    admin.query(f"DROP DATABASE IF EXISTS {DATABASE}")
    try:
        admin.query(f"CREATE DATABASE {DATABASE}")
        with NativeSessionClient(database=DATABASE) as client:
            dirty(client)
            client.query("USE system")
            admin.query(f"DROP DATABASE {DATABASE}")
            expect_error("UNKNOWN_DATABASE", client.reset_session)
            check_retained(client, "system")
            print("missing handshake database preserves the entire old session")

            admin.query(f"CREATE DATABASE {DATABASE}")
            client.reset_session()
            assert client.value("SELECT currentDatabase()") == DATABASE
            expect_error("UNKNOWN_TABLE", client.query, "SELECT x FROM reset_kept")
            expect_error(
                "UNKNOWN_QUERY_PARAMETER", client.query, "SELECT {kept:String}"
            )
            print("reset succeeds after repairing the handshake database")

        admin.query(f"CREATE USER {USER}")
        admin.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {USER}")
        admin.query(f"GRANT TABLE ENGINE ON Memory TO {USER}")
        with NativeSessionClient(user=USER) as client:
            dirty(client)
            admin.query(f"ALTER USER {USER} SETTINGS database = 'system' READONLY")
            expect_error("SETTING_CONSTRAINT_VIOLATION", client.reset_session)
            check_retained(client, CLICKHOUSE_DATABASE)
            print("new database constraint preserves the entire old session")
    finally:
        admin.query(f"DROP USER IF EXISTS {USER}")
        admin.query(f"DROP DATABASE IF EXISTS {DATABASE}")
