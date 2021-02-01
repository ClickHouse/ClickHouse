import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_test_keeper1.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/enable_test_keeper2.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'])
node3 = cluster.add_instance('node3', main_configs=['configs/enable_test_keeper3.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'])

from kazoo.client import KazooClient

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def smaller_exception(ex):
    return '\n'.join(str(ex).split('\n')[0:2])

def test_simple_replicated_table(started_cluster):

    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE TABLE t (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t', '{}') ORDER BY tuple()".format(i + 1))

    node2.query("INSERT INTO t SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA t", timeout=10)
    node3.query("SYSTEM SYNC REPLICA t", timeout=10)

    assert node1.query("SELECT COUNT() FROM t") == "10\n"
    assert node2.query("SELECT COUNT() FROM t") == "10\n"
    assert node3.query("SELECT COUNT() FROM t") == "10\n"



def test_blocade_leader(started_cluster):
    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE TABLE t1 (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t1', '{}') ORDER BY tuple()".format(i + 1))

    node2.query("INSERT INTO t1 SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA t1", timeout=10)
    node3.query("SYSTEM SYNC REPLICA t1", timeout=10)

    assert node1.query("SELECT COUNT() FROM t1") == "10\n"
    assert node2.query("SELECT COUNT() FROM t1") == "10\n"
    assert node3.query("SELECT COUNT() FROM t1") == "10\n"

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1)
        pm.partition_instances(node3, node1)

        for i in range(100):
            try:
                node2.query("SYSTEM RESTART REPLICA t1")
                node2.query("INSERT INTO t1 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node2.query("ATTACH TABLE t1")
                except Exception as attach_ex:
                    print("Got exception node2", smaller_exception(attach_ex))
                print("Got exception node2", smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot insert anything node2"

        for i in range(100):
            try:
                node3.query("SYSTEM RESTART REPLICA t1")
                node3.query("INSERT INTO t1 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node3.query("ATTACH TABLE t1")
                except Exception as attach_ex:
                    print("Got exception node3", smaller_exception(attach_ex))
                print("Got exception node3", smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot insert anything node3"

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA t1")
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE t1")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for i in range(100):
        try:
            node1.query("INSERT INTO t1 SELECT rand() FROM numbers(100)")
            break
        except Exception as ex:
            print("Got exception node1", smaller_exception(ex))
            time.sleep(0.5)
    else:
        assert False, "Cannot insert anything node1"

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM SYNC REPLICA t1", timeout=10)
                break
            except Exception as ex:
                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot sync replica node{}".format(n+1)

    assert node1.query("SELECT COUNT() FROM t1") == "310\n"
    assert node2.query("SELECT COUNT() FROM t1") == "310\n"
    assert node3.query("SELECT COUNT() FROM t1") == "310\n"


def test_blocade_leader_twice(started_cluster):
    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE TABLE t2 (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t2', '{}') ORDER BY tuple()".format(i + 1))

    node2.query("INSERT INTO t2 SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA t2", timeout=10)
    node3.query("SYSTEM SYNC REPLICA t2", timeout=10)

    assert node1.query("SELECT COUNT() FROM t2") == "10\n"
    assert node2.query("SELECT COUNT() FROM t2") == "10\n"
    assert node3.query("SELECT COUNT() FROM t2") == "10\n"

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1)
        pm.partition_instances(node3, node1)

        for i in range(100):
            try:
                node2.query("SYSTEM RESTART REPLICA t2")
                node2.query("INSERT INTO t2 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node2.query("ATTACH TABLE t2")
                except Exception as attach_ex:
                    print("Got exception node2", smaller_exception(attach_ex))
                print("Got exception node2", smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node2"

        for i in range(100):
            try:
                node3.query("SYSTEM RESTART REPLICA t2")
                node3.query("INSERT INTO t2 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node3.query("ATTACH TABLE t2")
                except Exception as attach_ex:
                    print("Got exception node3", smaller_exception(attach_ex))
                print("Got exception node3", smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node3"


        # Total network partition
        pm.partition_instances(node3, node2)

        for i in range(30):
            try:
                node3.query("INSERT INTO t2 SELECT rand() FROM numbers(100)")
                assert False, "Node3 became leader?"
            except Exception as ex:
                time.sleep(0.5)

        for i in range(30):
            try:
                node2.query("INSERT INTO t2 SELECT rand() FROM numbers(100)")
                assert False, "Node2 became leader?"
            except Exception as ex:
                time.sleep(0.5)


    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA t2")
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE t2")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("INSERT INTO t2 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for node in [node1, node2, node3]:
        for i in range(100):
            try:
                node.query("SYSTEM SYNC REPLICA t2", timeout=10)
                break
            except Exception as ex:
                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node{}".format(n + 1)

    assert node1.query("SELECT COUNT() FROM t2") == "510\n"
    assert node2.query("SELECT COUNT() FROM t2") == "510\n"
    assert node3.query("SELECT COUNT() FROM t2") == "510\n"
