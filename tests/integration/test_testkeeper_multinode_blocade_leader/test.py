import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_test_keeper1.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'], stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_test_keeper2.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'], stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_test_keeper3.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'], stay_alive=True)

from kazoo.client import KazooClient, KazooState

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def smaller_exception(ex):
    return '\n'.join(str(ex).split('\n')[0:2])

def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(node.name, timeout=30.0)
            zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            if zk:
                zk.stop()
                zk.close()
    else:
        raise Exception("Can't wait node", node.name, "to become ready")

def wait_nodes():
    for node in [node1, node2, node3]:
        wait_node(node)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout)
    def reset_listener(state):
        nonlocal _fake_zk_instance
        print("Fake zk callback called for state", state)
        if state != KazooState.CONNECTED:
            _fake_zk_instance._reset()

    _fake_zk_instance.add_listener(reset_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance


# in extremely rare case it can take more than 5 minutes in debug build with sanitizer
@pytest.mark.timeout(600)
def test_blocade_leader(started_cluster):
    wait_nodes()
    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE DATABASE IF NOT EXISTS ordinary ENGINE=Ordinary")
        node.query("CREATE TABLE ordinary.t1 (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t1', '{}') ORDER BY tuple()".format(i + 1))

    node2.query("INSERT INTO ordinary.t1 SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA ordinary.t1", timeout=10)
    node3.query("SYSTEM SYNC REPLICA ordinary.t1", timeout=10)

    assert node1.query("SELECT COUNT() FROM ordinary.t1") == "10\n"
    assert node2.query("SELECT COUNT() FROM ordinary.t1") == "10\n"
    assert node3.query("SELECT COUNT() FROM ordinary.t1") == "10\n"

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1)
        pm.partition_instances(node3, node1)

        for i in range(100):
            try:
                node2.query("SYSTEM RESTART REPLICA ordinary.t1")
                node2.query("INSERT INTO ordinary.t1 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node2.query("ATTACH TABLE ordinary.t1")
                except Exception as attach_ex:
                    print("Got exception node2", smaller_exception(attach_ex))
                print("Got exception node2", smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t1', '/clickhouse/t1/replicas/{}'.format(num + 1))
            assert False, "Cannot insert anything node2"

        for i in range(100):
            try:
                node3.query("SYSTEM RESTART REPLICA ordinary.t1")
                node3.query("INSERT INTO ordinary.t1 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node3.query("ATTACH TABLE ordinary.t1")
                except Exception as attach_ex:
                    print("Got exception node3", smaller_exception(attach_ex))
                print("Got exception node3", smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t1', '/clickhouse/t1/replicas/{}'.format(num + 1))
            assert False, "Cannot insert anything node3"

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA ordinary.t1")
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t1")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for i in range(100):
        try:
            node1.query("INSERT INTO ordinary.t1 SELECT rand() FROM numbers(100)")
            break
        except Exception as ex:
            print("Got exception node1", smaller_exception(ex))
            time.sleep(0.5)
    else:
        for num, node in enumerate([node1, node2, node3]):
            dump_zk(node, '/clickhouse/t1', '/clickhouse/t1/replicas/{}'.format(num + 1))
        assert False, "Cannot insert anything node1"

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA ordinary.t1")
                node.query("SYSTEM SYNC REPLICA ordinary.t1", timeout=10)
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t1")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t1', '/clickhouse/t1/replicas/{}'.format(num + 1))
            assert False, "Cannot sync replica node{}".format(n+1)

    if node1.query("SELECT COUNT() FROM ordinary.t1") != "310\n":
        for num, node in enumerate([node1, node2, node3]):
            dump_zk(node, '/clickhouse/t1', '/clickhouse/t1/replicas/{}'.format(num + 1))

    assert node1.query("SELECT COUNT() FROM ordinary.t1") == "310\n"
    assert node2.query("SELECT COUNT() FROM ordinary.t1") == "310\n"
    assert node3.query("SELECT COUNT() FROM ordinary.t1") == "310\n"


def dump_zk(node, zk_path, replica_path):
    print(node.query("SELECT * FROM system.replication_queue FORMAT Vertical"))
    print("Replicas")
    print(node.query("SELECT * FROM system.replicas FORMAT Vertical"))
    print("Replica 2 info")
    print(node.query("SELECT * FROM system.zookeeper WHERE path = '{}' FORMAT Vertical".format(zk_path)))
    print("Queue")
    print(node.query("SELECT * FROM system.zookeeper WHERE path = '{}/queue' FORMAT Vertical".format(replica_path)))
    print("Log")
    print(node.query("SELECT * FROM system.zookeeper WHERE path = '{}/log' FORMAT Vertical".format(zk_path)))
    print("Parts")
    print(node.query("SELECT name FROM system.zookeeper WHERE path = '{}/parts' FORMAT Vertical".format(replica_path)))

# in extremely rare case it can take more than 5 minutes in debug build with sanitizer
@pytest.mark.timeout(600)
def test_blocade_leader_twice(started_cluster):
    wait_nodes()
    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE DATABASE IF NOT EXISTS ordinary ENGINE=Ordinary")
        node.query("CREATE TABLE ordinary.t2 (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t2', '{}') ORDER BY tuple()".format(i + 1))

    node2.query("INSERT INTO ordinary.t2 SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)
    node3.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)

    assert node1.query("SELECT COUNT() FROM ordinary.t2") == "10\n"
    assert node2.query("SELECT COUNT() FROM ordinary.t2") == "10\n"
    assert node3.query("SELECT COUNT() FROM ordinary.t2") == "10\n"

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1)
        pm.partition_instances(node3, node1)

        for i in range(100):
            try:
                node2.query("SYSTEM RESTART REPLICA ordinary.t2")
                node2.query("INSERT INTO ordinary.t2 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node2.query("ATTACH TABLE ordinary.t2")
                except Exception as attach_ex:
                    print("Got exception node2", smaller_exception(attach_ex))
                print("Got exception node2", smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node2"

        for i in range(100):
            try:
                node3.query("SYSTEM RESTART REPLICA ordinary.t2")
                node3.query("INSERT INTO ordinary.t2 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                try:
                    node3.query("ATTACH TABLE ordinary.t2")
                except Exception as attach_ex:
                    print("Got exception node3", smaller_exception(attach_ex))
                print("Got exception node3", smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node3"


        # Total network partition
        pm.partition_instances(node3, node2)

        for i in range(10):
            try:
                node3.query("INSERT INTO ordinary.t2 SELECT rand() FROM numbers(100)")
                assert False, "Node3 became leader?"
            except Exception as ex:
                time.sleep(0.5)

        for i in range(10):
            try:
                node2.query("INSERT INTO ordinary.t2 SELECT rand() FROM numbers(100)")
                assert False, "Node2 became leader?"
            except Exception as ex:
                time.sleep(0.5)


    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA ordinary.t2")
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t2")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("INSERT INTO ordinary.t2 SELECT rand() FROM numbers(100)")
                break
            except Exception as ex:
                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA ordinary.t2")
                node.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t2")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node{}".format(n + 1)

    assert node1.query("SELECT COUNT() FROM ordinary.t2") == "510\n"
    if node2.query("SELECT COUNT() FROM ordinary.t2") != "510\n":
        for num, node in enumerate([node1, node2, node3]):
            dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))

    assert node2.query("SELECT COUNT() FROM ordinary.t2") == "510\n"
    assert node3.query("SELECT COUNT() FROM ordinary.t2") == "510\n"
