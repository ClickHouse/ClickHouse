import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

dictionary_node = cluster.add_instance("dictionary_node")
main_node = cluster.add_instance(
    "main_node",
    dictionaries=["configs/dictionaries/cache_ints_dictionary.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        dictionary_node.query("CREATE DATABASE IF NOT EXISTS test")
        dictionary_node.query(
            "CREATE TABLE test.ints "
            "(key UInt64, "
            "i8 Int8, i16 Int16, i32 Int32, i64 Int64, "
            "u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64) "
            "ENGINE = Memory"
        )
        dictionary_node.query(
            "INSERT INTO test.ints VALUES (7, 7, 7, 7, 7, 7, 7, 7, 7)"
        )

        yield cluster
    finally:
        cluster.shutdown()


def keys_requested():
    return int(
        main_node.query(
            "SELECT value FROM system.events WHERE event = 'DictCacheKeysRequested'"
        )
    )


def test_shutdown_during_update(started_cluster):
    query = "SELECT dictGet('experimental_dict', 'i8', toUInt64(7))"
    assert main_node.query(query) == "7\n"

    # The dictionary's max lifetime is 2 seconds.
    time.sleep(3)

    before = keys_requested()
    main_node.query("SYSTEM ENABLE FAILPOINT cache_dictionary_update_sleep")

    # Returns the expired value and queues an update, which sleeps in the update thread.
    assert main_node.query(query) == "7\n"

    deadline = time.monotonic() + 30
    while keys_requested() == before:
        assert time.monotonic() < deadline, "The update did not start"
        time.sleep(0.1)

    # The server must wait for the update while everything it uses is still alive.
    assert main_node.stop_clickhouse(stop_wait_sec=60) is True
    assert not main_node.contains_in_log("<Fatal>", from_host=True)

    main_node.start_clickhouse()
