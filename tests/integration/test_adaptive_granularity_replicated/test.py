import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)
node3 = cluster.add_instance('node3', with_zookeeper=True, image='yandex/clickhouse-server', tag='19.1.14',
                             with_installed_binary=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        time.sleep(1)

        yield cluster
    finally:
        cluster.shutdown()


def test_creating_table_different_setting(start_cluster):
    node1.query(
        "CREATE TABLE t1 (c1 String, c2 String) ENGINE=ReplicatedMergeTree('/clickhouse/t1', '1') ORDER BY tuple(c1) SETTINGS index_granularity_bytes = 0")
    node1.query("INSERT INTO t1 VALUES('x', 'y')")

    node2.query(
        "CREATE TABLE t1 (c1 String, c2 String) ENGINE=ReplicatedMergeTree('/clickhouse/t1', '2') ORDER BY tuple(c1) SETTINGS enable_mixed_granularity_parts = 0")

    node1.query("INSERT INTO t1 VALUES('a', 'b')")
    node2.query("SYSTEM SYNC REPLICA t1", timeout=5)

    node1.query("SELECT count() FROM t1") == "2\n"
    node2.query("SELECT count() FROM t1") == "1\n"

    node2.query("INSERT INTO t1 VALUES('c', 'd')")
    node1.query("SYSTEM SYNC REPLICA t1", timeout=5)

    # replication works
    node1.query("SELECT count() FROM t1") == "3\n"
    node2.query("SELECT count() FROM t1") == "2\n"

    # OPTIMIZE also works correctly
    node2.query("OPTIMIZE TABLE t1 FINAL") == "3\n"
    node1.query("SYSTEM SYNC REPLICA t1", timeout=5)

    node1.query("SELECT count() FROM t1") == "3\n"
    node2.query("SELECT count() FROM t1") == "2\n"

    path_part = node1.query(
        "SELECT path FROM system.parts WHERE table = 't1' AND active=1 ORDER BY partition DESC LIMIT 1").strip()

    with pytest.raises(Exception):  # check that we have no adaptive files
        node1.exec_in_container(["bash", "-c", "find {p} -name '*.mrk2' | grep '.*'".format(p=path_part)])

    path_part = node2.query(
        "SELECT path FROM system.parts WHERE table = 't1' AND active=1 ORDER BY partition DESC LIMIT 1").strip()

    with pytest.raises(Exception):  # check that we have no adaptive files
        node2.exec_in_container(["bash", "-c", "find {p} -name '*.mrk2' | grep '.*'".format(p=path_part)])


def test_old_node_with_new_node(start_cluster):
    node3.query(
        "CREATE TABLE t2 (c1 String, c2 String) ENGINE=ReplicatedMergeTree('/clickhouse/t2', '3') ORDER BY tuple(c1)")
    node3.query("INSERT INTO t2 VALUES('x', 'y')")

    node2.query(
        "CREATE TABLE t2 (c1 String, c2 String) ENGINE=ReplicatedMergeTree('/clickhouse/t2', '2') ORDER BY tuple(c1) SETTINGS enable_mixed_granularity_parts = 0")

    node3.query("INSERT INTO t2 VALUES('a', 'b')")
    node2.query("SYSTEM SYNC REPLICA t2", timeout=5)

    node3.query("SELECT count() FROM t2") == "2\n"
    node2.query("SELECT count() FROM t2") == "1\n"

    node2.query("INSERT INTO t2 VALUES('c', 'd')")
    node3.query("SYSTEM SYNC REPLICA t2", timeout=5)

    # replication works
    node3.query("SELECT count() FROM t2") == "3\n"
    node2.query("SELECT count() FROM t2") == "2\n"

    # OPTIMIZE also works correctly
    node3.query("OPTIMIZE table t2 FINAL")
    node2.query("SYSTEM SYNC REPLICA t2", timeout=5)

    node3.query("SELECT count() FROM t2") == "3\n"
    node2.query("SELECT count() FROM t2") == "2\n"

    path_part = node3.query(
        "SELECT path FROM system.parts WHERE table = 't2' AND active=1 ORDER BY partition DESC LIMIT 1").strip()

    with pytest.raises(Exception):  # check that we have no adaptive files
        node3.exec_in_container(["bash", "-c", "find {p} -name '*.mrk2' | grep '.*'".format(p=path_part)])

    path_part = node2.query(
        "SELECT path FROM system.parts WHERE table = 't2' AND active=1 ORDER BY partition DESC LIMIT 1").strip()

    with pytest.raises(Exception):  # check that we have no adaptive files
        node2.exec_in_container(["bash", "-c", "find {p} -name '*.mrk2' | grep '.*'".format(p=path_part)])
