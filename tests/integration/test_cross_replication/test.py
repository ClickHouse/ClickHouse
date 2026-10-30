import time
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node_to_shards = [
            (node1, [0, 2]),
            (node2, [0, 1]),
            (node3, [1, 2]),
        ]

        for node, shards in node_to_shards:
            for shard in shards:
                node.query(
                    """
CREATE DATABASE shard_{shard};

CREATE TABLE shard_{shard}.replicated(date Date, id UInt32, shard_id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
                """.format(
                        shard=shard, replica=node.name
                    )
                )

            node.query(
                """
CREATE TABLE distributed(date Date, id UInt32, shard_id UInt32)
    ENGINE = Distributed(test_cluster, '', replicated, shard_id);
"""
            )

        # Insert some data onto different shards using the Distributed table
        to_insert = """\
2017-06-16	111	0
2017-06-16	222	1
2017-06-16	333	2
"""
        node1.query("INSERT INTO distributed FORMAT TSV", stdin=to_insert)
        time.sleep(5)

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    # Check that the data has been inserted into correct tables.
    assert_eq_with_retry(node1, "SELECT id FROM shard_0.replicated", "111")
    assert_eq_with_retry(node1, "SELECT id FROM shard_2.replicated", "333")

    assert_eq_with_retry(node2, "SELECT id FROM shard_0.replicated", "111")
    assert_eq_with_retry(node2, "SELECT id FROM shard_1.replicated", "222")

    assert_eq_with_retry(node3, "SELECT id FROM shard_1.replicated", "222")
    assert_eq_with_retry(node3, "SELECT id FROM shard_2.replicated", "333")

    # Check that SELECT from the Distributed table works.
    # For now, serialize_query_plan is disabled, because all the tables must exist on initiator.
    expected_from_distributed = """\
2017-06-16	111	0
2017-06-16	222	1
2017-06-16	333	2
"""
    assert_eq_with_retry(
        node1, "SELECT * FROM distributed ORDER BY id settings serialize_query_plan = 0", expected_from_distributed
    )
    assert_eq_with_retry(
        node2, "SELECT * FROM distributed ORDER BY id settings serialize_query_plan = 0", expected_from_distributed
    )
    assert_eq_with_retry(
        node3, "SELECT * FROM distributed ORDER BY id settings serialize_query_plan = 0", expected_from_distributed
    )

    # Now isolate node3 from other nodes and check that SELECTs on other nodes still work.
    with PartitionManager() as pm:
        pm.partition_instances(node3, node1, action="REJECT --reject-with tcp-reset")
        pm.partition_instances(node3, node2, action="REJECT --reject-with tcp-reset")

        assert_eq_with_retry(
            node1, "SELECT * FROM distributed ORDER BY id settings serialize_query_plan = 0", expected_from_distributed
        )
        assert_eq_with_retry(
            node2, "SELECT * FROM distributed ORDER BY id settings serialize_query_plan = 0", expected_from_distributed
        )

        with pytest.raises(Exception):
            print(
                node3.query_with_retry(
                    "SELECT * FROM distributed ORDER BY id settings serialize_query_plan = 0", retry_count=5
                )
            )


def test_create_table_as_source_on_other_shards(started_cluster):
    # node1 hosts shards 0 and 2, so it does not have the shard_1 database. It ships the query to
    # shard_1_hosts, which holds only node2 and node3, the nodes that host shard_1. The legacy ON
    # CLUSTER formats carry the query as written, so the initiator must not read the source
    # definition it does not have.
    for node in (node2, node3):
        node.query(
            "CREATE TABLE shard_1.as_source(date Date, id UInt32, shard_id UInt32) "
            "ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id"
        )

    assert node1.query("EXISTS DATABASE shard_1") == "0\n"

    for version in (1, 2):
        table = "as_copy_v{}".format(version)
        node1.query(
            "CREATE TABLE shard_1.{} ON CLUSTER shard_1_hosts AS shard_1.as_source".format(table),
            settings={
                "distributed_ddl_entry_format_version": version,
                "distributed_ddl_output_mode": "throw",
            },
        )
        for node in (node2, node3):
            assert node.query("EXISTS TABLE shard_1.{}".format(table)) == "1\n"
            assert (
                node.query(
                    "SELECT engine FROM system.tables WHERE database = 'shard_1' AND name = '{}'".format(table)
                )
                == "MergeTree\n"
            )


if __name__ == "__main__":
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
