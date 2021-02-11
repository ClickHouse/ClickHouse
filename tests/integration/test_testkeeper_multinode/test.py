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

def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout)
    def reset_last_zxid_listener(state):
        print("Fake zk callback called for state", state)
        _fake_zk_instance.last_zxid = 0

        _fake_zk_instance.add_listener(reset_last_zxid_listener)
    _fake_zk_instance.start()
    return _fake_zk_instance

def test_read_write_multinode(started_cluster):
    try:
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.create("/test_read_write_multinode_node1", b"somedata1")
        node2_zk.create("/test_read_write_multinode_node2", b"somedata2")
        node3_zk.create("/test_read_write_multinode_node3", b"somedata3")

        # stale reads are allowed
        while node1_zk.exists("/test_read_write_multinode_node2") is None:
            time.sleep(0.1)

        while node1_zk.exists("/test_read_write_multinode_node3") is None:
            time.sleep(0.1)

        while node2_zk.exists("/test_read_write_multinode_node3") is None:
            time.sleep(0.1)

        assert node3_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node2_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"
        assert node1_zk.get("/test_read_write_multinode_node1")[0] == b"somedata1"

        assert node3_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node2_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"
        assert node1_zk.get("/test_read_write_multinode_node2")[0] == b"somedata2"

        assert node3_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"
        assert node2_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"
        assert node1_zk.get("/test_read_write_multinode_node3")[0] == b"somedata3"

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_watch_on_follower(started_cluster):
    try:
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.create("/test_data_watches")
        node2_zk.set("/test_data_watches", b"hello")
        node3_zk.set("/test_data_watches", b"world")

        node1_data = None
        def node1_callback(event):
            print("node1 data watch called")
            nonlocal node1_data
            node1_data = event

        node1_zk.get("/test_data_watches", watch=node1_callback)

        node2_data = None
        def node2_callback(event):
            print("node2 data watch called")
            nonlocal node2_data
            node2_data = event

        node2_zk.get("/test_data_watches", watch=node2_callback)

        node3_data = None
        def node3_callback(event):
            print("node3 data watch called")
            nonlocal node3_data
            node3_data = event

        node3_zk.get("/test_data_watches", watch=node3_callback)

        node1_zk.set("/test_data_watches", b"somevalue")
        time.sleep(3)

        print(node1_data)
        print(node2_data)
        print(node3_data)

        assert node1_data == node2_data
        assert node3_data == node2_data

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_session_expiration(started_cluster):
    try:
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3", timeout=3.0)

        node3_zk.create("/test_ephemeral_node", b"world", ephemeral=True)

        with PartitionManager() as pm:
            pm.partition_instances(node3, node2)
            pm.partition_instances(node3, node1)
            node3_zk.stop()
            node3_zk.close()
            for _ in range(100):
                if node1_zk.exists("/test_ephemeral_node") is None and node2_zk.exists("/test_ephemeral_node") is None:
                    break
                time.sleep(0.1)

        assert node1_zk.exists("/test_ephemeral_node") is None
        assert node2_zk.exists("/test_ephemeral_node") is None

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass


def test_follower_restart(started_cluster):
    try:
        node1_zk = get_fake_zk("node1")

        node1_zk.create("/test_restart_node", b"hello")

        node3.restart_clickhouse(kill=True)

        node3_zk = get_fake_zk("node3")

        # got data from log
        assert node3_zk.get("/test_restart_node")[0] == b"hello"

    finally:
        try:
            for zk_conn in [node1_zk, node3_zk]:
                try:
                    zk_conn.stop()
                    zk_conn.close()
                except:
                    pass
        except:
            pass


def test_simple_replicated_table(started_cluster):
    # something may be wrong after partition in other tests
    # so create with retry
    for i, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("CREATE TABLE t (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t', '{}') ORDER BY tuple()".format(i + 1))
                break
            except:
                time.sleep(0.1)

    node2.query("INSERT INTO t SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA t", timeout=10)
    node3.query("SYSTEM SYNC REPLICA t", timeout=10)

    assert node1.query("SELECT COUNT() FROM t") == "10\n"
    assert node2.query("SELECT COUNT() FROM t") == "10\n"
    assert node3.query("SELECT COUNT() FROM t") == "10\n"


# in extremely rare case it can take more than 5 minutes in debug build with sanitizer
@pytest.mark.timeout(600)
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
                node.query("SYSTEM RESTART REPLICA t1", timeout=10)
                node.query("SYSTEM SYNC REPLICA t1", timeout=10)
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE t1")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            assert False, "Cannot sync replica node{}".format(n+1)

    assert node1.query("SELECT COUNT() FROM t1") == "310\n"
    assert node2.query("SELECT COUNT() FROM t1") == "310\n"
    assert node3.query("SELECT COUNT() FROM t1") == "310\n"


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
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
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
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node3"


        # Total network partition
        pm.partition_instances(node3, node2)

        for i in range(10):
            try:
                node3.query("INSERT INTO t2 SELECT rand() FROM numbers(100)")
                assert False, "Node3 became leader?"
            except Exception as ex:
                time.sleep(0.5)

        for i in range(10):
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
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
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
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node{}".format(n + 1)

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                node.query("SYSTEM RESTART REPLICA t2")
                node.query("SYSTEM SYNC REPLICA t2", timeout=10)
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE t2")
                except Exception as attach_ex:
                    print("Got exception node{}".format(n + 1), smaller_exception(attach_ex))

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))
            assert False, "Cannot reconnect for node{}".format(n + 1)

    assert node1.query("SELECT COUNT() FROM t2") == "510\n"
    if node2.query("SELECT COUNT() FROM t2") != "510\n":
        for num, node in enumerate([node1, node2, node3]):
            dump_zk(node, '/clickhouse/t2', '/clickhouse/t2/replicas/{}'.format(num + 1))

    assert node2.query("SELECT COUNT() FROM t2") == "510\n"
    assert node3.query("SELECT COUNT() FROM t2") == "510\n"
