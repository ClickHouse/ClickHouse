from __future__ import print_function
import pytest
import time
import os

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.network import PartitionManagerDisabler

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))

dictionary_node = cluster.add_instance('dictionary_node', stay_alive=True)
main_node = cluster.add_instance('main_node', main_configs=['configs/dictionaries/cache_ints_dictionary.xml'])
pm = PartitionManager()

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        dictionary_node.query("create database if not exists test;")
        dictionary_node.query("drop table if exists test.ints;")
        dictionary_node.query("create table test.ints (key UInt64, i8 Int8, i16 Int16, i32 Int32, "
                              "i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64) "
                              "Engine = Memory;")
        dictionary_node.query("insert into test.ints values (7, 7, 7, 7, 7, 7, 7, 7, 7);")

        print(dictionary_node.query("describe test.ints"))
        print(dictionary_node.query("select * from test.ints"))

        yield cluster
    finally:
        cluster.shutdown()


def test_simple_dict_get(started_cluster):

    assert '7' == main_node.query("select dictGet('anime_dict', 'i8',  toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'i16', toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'i32', toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'i64', toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'u8',  toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'u16', toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'u32', toUInt64(7));").rstrip(), "Wrong answer."
    assert '7' == main_node.query("select dictGet('anime_dict', 'u64', toUInt64(7));").rstrip(), "Wrong answer."

    with PartitionManager() as pm:
        pm.partition_instances(main_node, dictionary_node, port=9000)

        # Kill the instance for sure
        dictionary_node.kill_clickhouse()
        assert None == dictionary_node.get_process_pid("clickhouse")
        pm.heal_all()
        pm.partition_instances(main_node, dictionary_node, port=9000)

        # print(dictionary_node.query("select * from test.ints"))
        assert '7' == main_node.query("select dictGet('anime_dict', 'i8',  toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'i16', toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'i32', toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'i64', toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'u8',  toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'u16', toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'u32', toUInt64(7));").rstrip(), "Wrong answer."
        assert '7' == main_node.query("select dictGet('anime_dict', 'u64', toUInt64(7));").rstrip(), "Wrong answer."
