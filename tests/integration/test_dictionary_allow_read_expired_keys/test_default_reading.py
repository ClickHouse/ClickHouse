from __future__ import print_function
import pytest
import time
import os
from contextlib import contextmanager

from helpers.cluster import ClickHouseCluster
from helpers.cluster import ClickHouseKiller
from helpers.network import PartitionManager

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))

dictionary_node = cluster.add_instance('dictionary_node', stay_alive=True)
main_node = cluster.add_instance('main_node', main_configs=['configs/dictionaries/cache_ints_dictionary.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        dictionary_node.query("create database if not exists test;")
        dictionary_node.query("drop table if exists test.ints;")
        dictionary_node.query("create table test.ints "
                              "(key UInt64, "
                              "i8 Int8,  i16 Int16,  i32 Int32,  i64 Int64, "
                              "u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64) "
                              "Engine = Memory;")
        dictionary_node.query("insert into test.ints values (7, 7, 7, 7, 7, 7, 7, 7, 7);")
        dictionary_node.query("insert into test.ints values (5, 5, 5, 5, 5, 5, 5, 5, 5);")

        yield cluster
    finally:
        cluster.shutdown()

# @pytest.mark.skip(reason="debugging")
def test_default_reading(started_cluster):
    assert None != dictionary_node.get_process_pid("clickhouse"), "ClickHouse must be alive"

    # Key 0 is not in dictionary, so default value will be returned

    def test_helper():
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'i8',  toUInt64(13),  toInt8(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'i16', toUInt64(13),  toInt16(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'i32', toUInt64(13),  toInt32(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'i64', toUInt64(13),  toInt64(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'u8',  toUInt64(13),  toUInt8(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'u16', toUInt64(13),  toUInt16(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'u32', toUInt64(13),  toUInt32(42));").rstrip()
        assert '42' == main_node.query("select dictGetOrDefault('anime_dict', 'u64', toUInt64(13),  toUInt64(42));").rstrip()

    test_helper()

    with PartitionManager() as pm, ClickHouseKiller(dictionary_node):
        assert None == dictionary_node.get_process_pid("clickhouse"), "CLickHouse must be alive"

        # Remove connection between main_node and dictionary for sure
        pm.heal_all()
        pm.partition_instances(main_node, dictionary_node)

        # Dictionary max lifetime is 2 seconds.
        time.sleep(3)

        test_helper()

