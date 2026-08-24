import os
import pytest

from kazoo.client import KazooClient, KazooRetry
from helpers.cluster import ClickHouseCluster

CLUSTER_SIZE = 3

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

ch_node = cluster.add_instance(f"node1", stay_alive=True, with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_recovery_with_flag(started_cluster):

    ch_node.query("DROP TABLE IF EXISTS test_recovery_with_flag SYNC")
    ch_node.query(
        """CREATE TABLE test_recovery_with_flag
        (
            num UInt32
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_recovery_with_flag', '1')
        ORDER BY num
    """
    )

    ch_node.query("SELECT count() FROM test_recovery_with_flag")

    is_readonly = ch_node.query(
        "SELECT is_readonly FROM system.replicas WHERE table = 'test_recovery_with_flag'"
    )
    assert is_readonly == "0\n"

    ch_node.stop_clickhouse()

    zk = cluster.get_kazoo_client("zoo1")
    zk.delete("/clickhouse", recursive=True)
    zk.stop()

    ch_node.start_clickhouse()
    ch_node.query("SELECT count() FROM test_recovery_with_flag")

    is_readonly = ch_node.query(
        "SELECT is_readonly FROM system.replicas WHERE table = 'test_recovery_with_flag'"
    )
    assert is_readonly == "1\n"

    ch_node.stop_clickhouse()
    ch_node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/flags/force_restore_data"],
        privileged=True,
    )

    ch_node.start_clickhouse()
    ch_node.query("SELECT count() FROM test_recovery_with_flag")

    is_readonly = ch_node.query(
        "SELECT is_readonly, zookeeper_exception FROM system.replicas WHERE table = 'test_recovery_with_flag'"
    )
    assert is_readonly == "0\t\n"

    ch_node.query("DROP TABLE IF EXISTS test_recovery_with_flag SYNC")
