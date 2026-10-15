#!/usr/bin/env python3

# The `ResetSession` packet of the native protocol returns the session to its state right after
# authentication, on the same connection. The test client uses a protocol revision that is lower
# than the revision of the packet, because the server accepts the packet from any client.

import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from native_session_client import (
    CLICKHOUSE_DATABASE,
    RESET_SESSION_REVISION,
    ConnectionClosed,
    NativeSessionClient,
    expect_error,
)

OTHER_DATABASE = f"{CLICKHOUSE_DATABASE}_reset_session"


def setting(client, name):
    return client.value(f"SELECT toString(getSetting('{name}'))")


def test_session_state():
    with NativeSessionClient() as client:
        print("server revision supports the packet:", client.server_revision >= RESET_SESSION_REVISION)

        max_threads = setting(client, "max_threads")
        max_block_size = setting(client, "max_block_size")

        client.query("SET max_threads = 3, max_block_size = 12345")
        client.query("SET param_x = 'mango'")
        client.query("CREATE TEMPORARY TABLE reset_session_tmp ENGINE = Memory AS SELECT 'one' AS s")
        client.query("USE system")
        print("before reset:", setting(client, "max_threads"), setting(client, "max_block_size"),
              client.value("SELECT {x:String}"), client.value("SELECT s FROM reset_session_tmp"),
              client.value("SELECT currentDatabase()"))

        client.reset_session()

        print("settings are restored:", setting(client, "max_threads") == max_threads,
              setting(client, "max_block_size") == max_block_size)
        expect_error("UNKNOWN_QUERY_PARAMETER", client.query, "SELECT {x:String}")
        expect_error("UNKNOWN_TABLE", client.query, "SELECT s FROM reset_session_tmp")
        print("database is restored:", client.value("SELECT currentDatabase()") == CLICKHOUSE_DATABASE)

        # A reset of a clean session changes nothing, and the session is usable after it.
        client.reset_session()
        client.reset_session()
        client.ping()
        client.query("CREATE TEMPORARY TABLE reset_session_tmp ENGINE = Memory AS SELECT 'two' AS s")
        print("after reset:", client.value("SELECT s FROM reset_session_tmp"))


def test_backup_in_memory():
    # `BACKUP ... TO Memory` keeps the backup in the session.
    with NativeSessionClient() as client:
        client.query("CREATE TABLE reset_session_src ENGINE = MergeTree ORDER BY x AS SELECT 1 AS x")
        client.query("BACKUP TABLE reset_session_src TO Memory('reset_session_backup')")
        expect_error("BACKUP_ALREADY_EXISTS", client.query,
                     "BACKUP TABLE reset_session_src TO Memory('reset_session_backup')")

        client.reset_session()

        client.query("BACKUP TABLE reset_session_src TO Memory('reset_session_backup')")
        print("in-memory backup is dropped")
        client.query("DROP TABLE reset_session_src")


def test_dropped_handshake_database():
    # When the database from the handshake is gone, the reset fails as a new connection would,
    # and the session stays unchanged.
    with NativeSessionClient() as admin:
        admin.query(f"CREATE DATABASE {OTHER_DATABASE}")
        with NativeSessionClient(database=OTHER_DATABASE) as client:
            client.query("SET max_block_size = 12345")
            admin.query(f"DROP DATABASE {OTHER_DATABASE}")

            expect_error("UNKNOWN_DATABASE", client.reset_session)
            print("session is unchanged after a failed reset:", setting(client, "max_block_size"))


def print_connection_state(client):
    try:
        client.ping()
        print("connection is open")
    except (ConnectionClosed, OSError):
        print("connection is closed")


def test_reset_while_reading_query_data():
    # The packet is accepted only between queries.
    with NativeSessionClient() as client:
        client.send_query("SELECT 1", end_external_tables=False)
        client.send_reset_session()
        expect_error("UNEXPECTED_PACKET_FROM_CLIENT", client.receive_result)

        # The server reads the rest of the query data, then closes the connection.
        client.send_empty_block()
        print_connection_state(client)


def test_reset_while_query_runs():
    # The server has read the query data and runs the query. It cancels the query.
    with NativeSessionClient() as client:
        client.send_query("SELECT toString(sleepEachRow(0.1)) FROM numbers(100) SETTINGS max_block_size = 1")
        client.send_reset_session()
        expect_error("UNEXPECTED_PACKET_FROM_CLIENT", client.receive_result)
        print_connection_state(client)


test_session_state()
test_backup_in_memory()
test_dropped_handshake_database()
test_reset_while_reading_query_data()
test_reset_while_query_runs()
