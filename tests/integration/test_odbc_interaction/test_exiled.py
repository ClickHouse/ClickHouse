import time
import logging
import pytest

from helpers.cluster import ClickHouseCluster, assert_eq_with_retry
from test_odbc_interaction.test import (
    create_mysql_db,
    create_mysql_table,
    get_mysql_conn,
    skip_test_msan,
)


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_odbc_drivers=True,
    main_configs=["configs/openssl.xml", "configs/odbc_logging.xml"],
    stay_alive=True,
    dictionaries=["configs/dictionaries/sqlite3_odbc_hashed_dictionary.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]
        logging.debug(f"sqlite data received: {sqlite_db}")
        node1.exec_in_container(
            [
                "sqlite3",
                sqlite_db,
                "CREATE TABLE t2(id INTEGER PRIMARY KEY ASC, X INTEGER, Y, Z);",
            ],
            privileged=True,
            user="root",
        )

        node1.exec_in_container(
            ["sqlite3", sqlite_db, "INSERT INTO t2 values(1, 1, 2, 3);"],
            privileged=True,
            user="root",
        )

        node1.query("SYSTEM RELOAD DICTIONARY sqlite3_odbc_hashed")

        yield cluster
    except Exception as ex:
        logging.exception(ex)
        raise ex
    finally:
        cluster.shutdown()


# This test kills ClickHouse server and ODBC bridge and in worst scenario
# may cause group test crashes. Thus, this test is executed in a separate "module"
# with separate environment.
def test_bridge_dies_with_parent(started_cluster):
    skip_test_msan(node1)

    if node1.is_built_with_address_sanitizer():
        # TODO: Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge in this test and
        # that's linked somehow with that we have replaced getauxval() in glibc-compatibility.
        # The leak sanitizer calls getauxval() for its own purposes, and our replaced version doesn't seem to be equivalent in that case.
        pytest.skip(
            "Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge"
        )

    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "3"
    )

    clickhouse_pid = node1.get_process_pid("clickhouse server")
    bridge_pid = node1.get_process_pid("odbc-bridge")
    assert clickhouse_pid is not None
    assert bridge_pid is not None

    try:
        node1.exec_in_container(
            ["kill", str(clickhouse_pid)], privileged=True, user="root"
        )
    except:
        pass

    for _ in range(30):
        time.sleep(1)
        clickhouse_pid = node1.get_process_pid("clickhouse server")
        if clickhouse_pid is None:
            break

    for _ in range(30):
        time.sleep(1)  # just for sure, that odbc-bridge caught signal
        bridge_pid = node1.get_process_pid("odbc-bridge")
        if bridge_pid is None:
            break

    if bridge_pid:
        out = node1.exec_in_container(
            ["gdb", "-p", str(bridge_pid), "--ex", "thread apply all bt", "--ex", "q"],
            privileged=True,
            user="root",
        )
        logging.debug(f"Bridge is running, gdb output:\n{out}")

    try:
        assert clickhouse_pid is None
        assert bridge_pid is None
    finally:
        node1.start_clickhouse(20)
