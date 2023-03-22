import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)

from kazoo.client import KazooClient, KazooState

"""
In this test, we blockade RAFT leader and check that the whole system is
able to recover. It's not a good test because we use ClickHouse's replicated
tables to check connectivity, but they may require special operations (or a long
wait) after session expiration. We don't use kazoo, because this client pretends
to be very smart: SUSPEND sessions, try to recover them, and so on. The test
will be even less predictable than with ClickHouse tables.

TODO find (or write) not so smart python client.
TODO remove this when jepsen tests will be written.
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def smaller_exception(ex):
    return "\n".join(str(ex).split("\n")[0:2])


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
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


# in extremely rare case it can take more than 5 minutes in debug build with sanitizer
@pytest.mark.timeout(600)
def test_blocade_leader(started_cluster):
    for i in range(100):
        wait_nodes()
        try:
            for i, node in enumerate([node1, node2, node3]):
                node.query(
                    "CREATE DATABASE IF NOT EXISTS ordinary ENGINE=Ordinary",
                    settings={"allow_deprecated_database_ordinary": 1},
                )
                node.query(
                    "CREATE TABLE IF NOT EXISTS ordinary.t1 (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t1', '{}') ORDER BY tuple()".format(
                        i + 1
                    )
                )
            break
        except Exception as ex:
            print("Got exception from node", smaller_exception(ex))
            time.sleep(0.1)

    node2.query("INSERT INTO ordinary.t1 SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA ordinary.t1", timeout=10)
    node3.query("SYSTEM SYNC REPLICA ordinary.t1", timeout=10)

    assert_eq_with_retry(node1, "SELECT COUNT() FROM ordinary.t1", "10")
    assert_eq_with_retry(node2, "SELECT COUNT() FROM ordinary.t1", "10")
    assert_eq_with_retry(node3, "SELECT COUNT() FROM ordinary.t1", "10")

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1)
        pm.partition_instances(node3, node1)

        for i in range(100):
            try:
                restart_replica_for_sure(
                    node2, "ordinary.t1", "/clickhouse/t1/replicas/2"
                )
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
                dump_zk(
                    node, "/clickhouse/t1", "/clickhouse/t1/replicas/{}".format(num + 1)
                )
            assert False, "Cannot insert anything node2"

        for i in range(100):
            try:
                restart_replica_for_sure(
                    node3, "ordinary.t1", "/clickhouse/t1/replicas/3"
                )
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
                dump_zk(
                    node, "/clickhouse/t1", "/clickhouse/t1/replicas/{}".format(num + 1)
                )
            assert False, "Cannot insert anything node3"

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                restart_replica_for_sure(
                    node, "ordinary.t1", "/clickhouse/t1/replicas/{}".format(n + 1)
                )
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t1")
                except Exception as attach_ex:
                    print(
                        "Got exception node{}".format(n + 1),
                        smaller_exception(attach_ex),
                    )

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
            dump_zk(
                node, "/clickhouse/t1", "/clickhouse/t1/replicas/{}".format(num + 1)
            )
        assert False, "Cannot insert anything node1"

    for n, node in enumerate([node1, node2, node3]):
        for i in range(100):
            try:
                restart_replica_for_sure(
                    node, "ordinary.t1", "/clickhouse/t1/replicas/{}".format(n + 1)
                )
                node.query("SYSTEM SYNC REPLICA ordinary.t1", timeout=10)
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t1")
                except Exception as attach_ex:
                    print(
                        "Got exception node{}".format(n + 1),
                        smaller_exception(attach_ex),
                    )

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(
                    node, "/clickhouse/t1", "/clickhouse/t1/replicas/{}".format(num + 1)
                )
            assert False, "Cannot sync replica node{}".format(n + 1)

    if node1.query("SELECT COUNT() FROM ordinary.t1") != "310\n":
        for num, node in enumerate([node1, node2, node3]):
            dump_zk(
                node, "/clickhouse/t1", "/clickhouse/t1/replicas/{}".format(num + 1)
            )

    assert_eq_with_retry(node1, "SELECT COUNT() FROM ordinary.t1", "310")
    assert_eq_with_retry(node2, "SELECT COUNT() FROM ordinary.t1", "310")
    assert_eq_with_retry(node3, "SELECT COUNT() FROM ordinary.t1", "310")


def dump_zk(node, zk_path, replica_path):
    print(node.query("SELECT * FROM system.replication_queue FORMAT Vertical"))
    print("Replicas")
    print(node.query("SELECT * FROM system.replicas FORMAT Vertical"))
    print("Replica 2 info")
    print(
        node.query(
            "SELECT * FROM system.zookeeper WHERE path = '{}' FORMAT Vertical".format(
                zk_path
            )
        )
    )
    print("Queue")
    print(
        node.query(
            "SELECT * FROM system.zookeeper WHERE path = '{}/queue' FORMAT Vertical".format(
                replica_path
            )
        )
    )
    print("Log")
    print(
        node.query(
            "SELECT * FROM system.zookeeper WHERE path = '{}/log' FORMAT Vertical".format(
                zk_path
            )
        )
    )
    print("Parts")
    print(
        node.query(
            "SELECT name FROM system.zookeeper WHERE path = '{}/parts' FORMAT Vertical".format(
                replica_path
            )
        )
    )


def restart_replica_for_sure(node, table_name, zk_replica_path):
    fake_zk = None
    try:
        node.query("DETACH TABLE {}".format(table_name))
        fake_zk = get_fake_zk(node.name)
        if fake_zk.exists(zk_replica_path + "/is_active") is not None:
            fake_zk.delete(zk_replica_path + "/is_active")

        node.query("ATTACH TABLE {}".format(table_name))
    except Exception as ex:
        print("Exception", ex)
        raise ex
    finally:
        if fake_zk:
            fake_zk.stop()
            fake_zk.close()


# in extremely rare case it can take more than 5 minutes in debug build with sanitizer
@pytest.mark.timeout(600)
def test_blocade_leader_twice(started_cluster):
    for i in range(100):
        wait_nodes()
        try:
            for i, node in enumerate([node1, node2, node3]):
                node.query(
                    "CREATE DATABASE IF NOT EXISTS ordinary ENGINE=Ordinary",
                    settings={"allow_deprecated_database_ordinary": 1},
                )
                node.query(
                    "CREATE TABLE IF NOT EXISTS ordinary.t2 (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t2', '{}') ORDER BY tuple()".format(
                        i + 1
                    )
                )
            break
        except Exception as ex:
            print("Got exception from node", smaller_exception(ex))
            time.sleep(0.1)

    node2.query("INSERT INTO ordinary.t2 SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)
    node3.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)

    assert_eq_with_retry(node1, "SELECT COUNT() FROM ordinary.t2", "10")
    assert_eq_with_retry(node2, "SELECT COUNT() FROM ordinary.t2", "10")
    assert_eq_with_retry(node3, "SELECT COUNT() FROM ordinary.t2", "10")

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1)
        pm.partition_instances(node3, node1)

        for i in range(100):
            try:
                restart_replica_for_sure(
                    node2, "ordinary.t2", "/clickhouse/t2/replicas/2"
                )
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
                dump_zk(
                    node, "/clickhouse/t2", "/clickhouse/t2/replicas/{}".format(num + 1)
                )
            assert False, "Cannot reconnect for node2"

        for i in range(100):
            try:
                restart_replica_for_sure(
                    node3, "ordinary.t2", "/clickhouse/t2/replicas/3"
                )
                node3.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)
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
                dump_zk(
                    node, "/clickhouse/t2", "/clickhouse/t2/replicas/{}".format(num + 1)
                )
            assert False, "Cannot reconnect for node3"

        node2.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)

        assert_eq_with_retry(node2, "SELECT COUNT() FROM ordinary.t2", "210")
        assert_eq_with_retry(node3, "SELECT COUNT() FROM ordinary.t2", "210")

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
                restart_replica_for_sure(
                    node, "ordinary.t2", "/clickhouse/t2/replicas/{}".format(n + 1)
                )
                break
            except Exception as ex:
                try:
                    node.query("ATTACH TABLE ordinary.t2")
                except Exception as attach_ex:
                    print(
                        "Got exception node{}".format(n + 1),
                        smaller_exception(attach_ex),
                    )

                print("Got exception node{}".format(n + 1), smaller_exception(ex))
                time.sleep(0.5)
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(
                    node, "/clickhouse/t2", "/clickhouse/t2/replicas/{}".format(num + 1)
                )
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
                dump_zk(
                    node, "/clickhouse/t2", "/clickhouse/t2/replicas/{}".format(num + 1)
                )
            assert False, "Cannot reconnect for node{}".format(n + 1)

        for i in range(100):
            all_done = True
            for n, node in enumerate([node1, node2, node3]):
                try:
                    restart_replica_for_sure(
                        node, "ordinary.t2", "/clickhouse/t2/replicas/{}".format(n + 1)
                    )
                    node.query("SYSTEM SYNC REPLICA ordinary.t2", timeout=10)
                    break
                except Exception as ex:
                    all_done = False
                    try:
                        node.query("ATTACH TABLE ordinary.t2")
                    except Exception as attach_ex:
                        print(
                            "Got exception node{}".format(n + 1),
                            smaller_exception(attach_ex),
                        )

                    print("Got exception node{}".format(n + 1), smaller_exception(ex))
                    time.sleep(0.5)

            if all_done:
                break
        else:
            for num, node in enumerate([node1, node2, node3]):
                dump_zk(
                    node, "/clickhouse/t2", "/clickhouse/t2/replicas/{}".format(num + 1)
                )
            assert False, "Cannot reconnect in i {} retries".format(i)

    assert_eq_with_retry(node1, "SELECT COUNT() FROM ordinary.t2", "510")
    if node2.query("SELECT COUNT() FROM ordinary.t2") != "510\n":
        for num, node in enumerate([node1, node2, node3]):
            dump_zk(
                node, "/clickhouse/t2", "/clickhouse/t2/replicas/{}".format(num + 1)
            )

    assert_eq_with_retry(node2, "SELECT COUNT() FROM ordinary.t2", "510")
    assert_eq_with_retry(node3, "SELECT COUNT() FROM ordinary.t2", "510")
