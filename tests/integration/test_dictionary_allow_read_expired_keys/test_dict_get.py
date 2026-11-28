import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseKiller
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

dictionary_node = cluster.add_instance("dictionary_node", stay_alive=True)
main_node = cluster.add_instance(
    "main_node",
    dictionaries=["configs/dictionaries/cache_ints_dictionary.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        dictionary_node.query("create database if not exists test;")
        dictionary_node.query("drop table if exists test.ints;")
        dictionary_node.query(
            "create table test.ints "
            "(key UInt64, "
            "i8 Int8,  i16 Int16,  i32 Int32,  i64 Int64, "
            "u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64) "
            "Engine = Memory;"
        )
        dictionary_node.query(
            "insert into test.ints values (7, 7, 7, 7, 7, 7, 7, 7, 7);"
        )
        dictionary_node.query(
            "insert into test.ints values (5, 5, 5, 5, 5, 5, 5, 5, 5);"
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_simple_dict_get(started_cluster):
    assert None != dictionary_node.get_process_pid(
        "clickhouse"
    ), "ClickHouse must be alive"

    def test_helper():
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'i8',  toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'i16', toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'i32', toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'i64', toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'u8',  toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'u16', toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'u32', toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."
        assert (
            "7"
            == main_node.query(
                "select dictGet('experimental_dict', 'u64', toUInt64(7));"
            ).rstrip()
        ), "Wrong answer."

    test_helper()

    with PartitionManager() as pm, ClickHouseKiller(dictionary_node):
        assert None == dictionary_node.get_process_pid("clickhouse")

        # Remove connection between main_node and dictionary for sure
        pm.heal_all()
        pm.partition_instances(main_node, dictionary_node)

        # Dictionary max lifetime is 2 seconds.
        time.sleep(3)

        test_helper()


def keys_requested():
    return int(
        main_node.query(
            "SELECT value FROM system.events WHERE event = 'DictCacheKeysRequested'"
        )
    )


def test_shutdown_during_update(started_cluster):
    # test_simple_dict_get restarts dictionary_node, which empties its Memory table,
    # and leaves the dictionary in the update error backoff.
    dictionary_node.query("INSERT INTO test.ints VALUES (9, 9, 9, 9, 9, 9, 9, 9, 9)")
    main_node.query("SYSTEM RELOAD DICTIONARY experimental_dict")

    query = "SELECT dictGet('experimental_dict', 'i8', toUInt64(9))"
    assert main_node.query(query) == "9\n"

    # The dictionary's max lifetime is 2 seconds.
    time.sleep(3)

    before = keys_requested()
    main_node.query("SYSTEM ENABLE FAILPOINT cache_dictionary_update_sleep")

    # Returns the expired value and queues an update, which sleeps in the update thread.
    assert main_node.query(query) == "9\n"

    deadline = time.monotonic() + 30
    while keys_requested() == before:
        assert time.monotonic() < deadline, "The update did not start"
        time.sleep(0.1)

    # The server must wait for the update while everything it uses is still alive.
    assert main_node.stop_clickhouse(stop_wait_sec=60) is True
    assert not main_node.contains_in_log("<Fatal>", from_host=True)

    main_node.start_clickhouse()
