import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

instance_test_reconnect = cluster.add_instance(
    "instance_test_reconnect", main_configs=["configs/remote_servers.xml"]
)
instance_test_inserts_batching = cluster.add_instance(
    "instance_test_inserts_batching",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/enable_distributed_inserts_batching.xml"],
)
remote = cluster.add_instance(
    "remote", main_configs=["configs/forbid_background_merges.xml"]
)

instance_test_inserts_local_cluster = cluster.add_instance(
    "instance_test_inserts_local_cluster", main_configs=["configs/remote_servers.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        remote.query("CREATE TABLE local1 (x UInt32) ENGINE = Log")

        instance_test_reconnect.query(
            """
CREATE TABLE distributed (x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local1')
"""
        )
        instance_test_reconnect.query(
            "CREATE TABLE local1_source (x UInt32) ENGINE = Memory"
        )
        instance_test_reconnect.query(
            "CREATE MATERIALIZED VIEW local1_view to distributed AS SELECT x FROM local1_source"
        )

        remote.query(
            "CREATE TABLE local2 (d Date, x UInt32, s String) ENGINE = MergeTree(d, x, 8192)",
            settings={"allow_deprecated_syntax_for_merge_tree": 1},
        )
        instance_test_inserts_batching.query(
            """
CREATE TABLE distributed (d Date, x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local2')
"""
        )
        instance_test_inserts_batching.query(
            "CREATE TABLE local2_source (d Date, x UInt32) ENGINE = Log"
        )
        instance_test_inserts_batching.query(
            "CREATE MATERIALIZED VIEW local2_view to distributed AS SELECT d,x FROM local2_source"
        )

        instance_test_inserts_local_cluster.query(
            "CREATE TABLE local_source (d Date, x UInt32) ENGINE = Memory"
        )
        instance_test_inserts_local_cluster.query(
            "CREATE MATERIALIZED VIEW local_view to distributed_on_local AS SELECT d,x FROM local_source"
        )
        instance_test_inserts_local_cluster.query(
            "CREATE TABLE local (d Date, x UInt32) ENGINE = MergeTree(d, x, 8192)",
            settings={"allow_deprecated_syntax_for_merge_tree": 1},
        )
        instance_test_inserts_local_cluster.query(
            """
CREATE TABLE distributed_on_local (d Date, x UInt32) ENGINE = Distributed('test_local_cluster', 'default', 'local')
"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_reconnect(started_cluster):
    instance = instance_test_reconnect

    with PartitionManager() as pm:
        # Open a connection for insertion.
        instance.query("INSERT INTO local1_source VALUES (1)")
        time.sleep(1)
        assert remote.query("SELECT count(*) FROM local1").strip() == "1"

        # Now break the connection.
        pm.partition_instances(
            instance, remote, action="REJECT --reject-with tcp-reset"
        )
        instance.query("INSERT INTO local1_source VALUES (2)")
        time.sleep(1)

        # Heal the partition and insert more data.
        # The connection must be reestablished and after some time all data must be inserted.
        pm.heal_all()
        time.sleep(1)

        instance.query("INSERT INTO local1_source VALUES (3)")
        time.sleep(1)

        assert remote.query("SELECT count(*) FROM local1").strip() == "3"


def test_inserts_local(started_cluster):
    instance = instance_test_inserts_local_cluster
    instance.query("INSERT INTO local_source VALUES ('2000-01-01', 1)")
    time.sleep(0.5)
    assert instance.query("SELECT count(*) FROM local").strip() == "1"
