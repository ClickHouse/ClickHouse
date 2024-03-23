import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.test_tools import TSV
from .cluster import ClickHouseClusterWithDDLHelpers


@pytest.fixture(scope="module")
def test_cluster():
    cluster = ClickHouseClusterWithDDLHelpers(__file__, "configs", "truncate")

    try:
        # TODO: Fix ON CLUSTER alters when nodes have different configs. Need to canonicalize node identity.
        cluster.prepare(replace_hostnames_with_ips=False)

        firewall_drops_rules = cluster.pm_random_drops.pop_rules()

        yield cluster

        # Enable random ZK packet drops
        cluster.pm_random_drops.push_rules(firewall_drops_rules)

        instance = cluster.instances["ch1"]

        cluster.ddl_check_query(
            instance, "DROP TABLE IF EXISTS replicated_table ON CLUSTER cluster SYNC"
        )

        cluster.ddl_check_query(
            instance, "DROP TABLE IF EXISTS dist_table ON CLUSTER cluster SYNC"
        )

        cluster.pm_random_drops.heal_all()

    finally:
        cluster.shutdown()


def test_truncate_over_replicated(test_cluster):
    instance = test_cluster.instances["ch2"]

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS replicated_table ON CLUSTER cluster SYNC"
    )

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS dist_table ON CLUSTER cluster SYNC"
    )

    # Temporarily disable random ZK packet drops, they might broke creation if ReplicatedMergeTree replicas
    firewall_drops_rules = test_cluster.pm_random_drops.pop_rules()

    test_cluster.ddl_check_query(
        instance,
        """
        CREATE TABLE IF NOT EXISTS replicated_table ON CLUSTER cluster (shard Int32, step Int32, src_node Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/replicated_table', '{replica}')
        ORDER BY (shard, step, src_node)
        """,
    )

    test_cluster.ddl_check_query(
        instance,
        """
        CREATE TABLE IF NOT EXISTS dist_table ON CLUSTER cluster (shard Int32, step Int32, src_node Int32)
        ENGINE = Distributed(cluster, default, replicated_table, shard%2)
        """,
    )

    select_query = """
        SELECT shard, step, src_node FROM dist_table
        ORDER BY shard, step, src_node
        """

    expected_data = []

    # async insert and sync replicated table
    for i in range(4):
        node_shard = i // 2 * 2
        dst_shard = (node_shard + 1) % 2
        test_cluster.instances["ch{}".format(i + 1)].query(
            f"INSERT INTO dist_table VALUES ({dst_shard}, 1, {i})",
            settings={"async_insert": "1", "wait_for_async_insert": "0"},
        )
        expected_data += [[dst_shard, 1, i]]

    instance.query("SYSTEM FLUSH DISTRIBUTED dist_table ON CLUSTER cluster")

    test_cluster.sync_replicas("replicated_table")

    assert TSV(instance.query(select_query)) == TSV(expected_data)

    # async insert with quorum 2, no need to sync replica, but still need to flush distributed
    for i in range(4):
        node_shard = i // 2 * 2
        dst_shard = (node_shard + 1) % 2
        test_cluster.instances["ch{}".format(i + 1)].query(
            f"INSERT INTO dist_table VALUES ({dst_shard}, 2, {i})",
            settings={
                "insert_quorum": "2",
                "async_insert": "1",
                "wait_for_async_insert": "0",
            },
        )
        expected_data += [[dst_shard, 2, i]]

    instance.query("SYSTEM FLUSH DISTRIBUTED dist_table ON CLUSTER cluster")

    assert TSV(instance.query(select_query)) == TSV(expected_data)

    # insert with quorum 3, queries would be stuck
    for i in range(4):
        node_shard = i // 2 * 2
        dst_shard = (node_shard + 1) % 2
        test_cluster.instances["ch{}".format(i + 1)].query(
            f"INSERT INTO dist_table VALUES ({dst_shard}, 3, {i})",
            settings={
                "insert_quorum": "3",
                "async_insert": "1",
                "wait_for_async_insert": "0",
            },
        )
        # write is failed
        # expected_data += [[dst_shard, 3, i]]

    error = instance.query_and_get_error(
        "SYSTEM FLUSH DISTRIBUTED dist_table ON CLUSTER cluster"
    )

    assert "Number of alive replicas (2) is less than requested quorum (3/2)" in error
    assert "TOO_FEW_LIVE_REPLICAS" in error

    assert TSV(instance.query(select_query)) == TSV(expected_data)

    instance.query("TRUNCATE TABLE dist_table ON CLUSTER cluster")

    instance.query("SYSTEM FLUSH DISTRIBUTED dist_table ON CLUSTER cluster")

    # insert after recover
    for i in range(4):
        node_shard = i // 2 * 2
        dst_shard = (node_shard + 1) % 2
        test_cluster.instances["ch{}".format(i + 1)].query(
            f"INSERT INTO dist_table VALUES ({dst_shard}, 4, {i})",
            settings={
                "insert_quorum": "2",
                "async_insert": "1",
                "wait_for_async_insert": "0",
            },
        )
        expected_data += [[dst_shard, 4, i]]

    instance.query("SYSTEM FLUSH DISTRIBUTED dist_table ON CLUSTER cluster")

    assert TSV(instance.query(select_query)) == TSV(expected_data)
