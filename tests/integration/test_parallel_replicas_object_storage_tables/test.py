import uuid

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/cluster.xml"], with_minio=True)
node2 = cluster.add_instance("node2", main_configs=["configs/cluster.xml"])
# Older than the cluster function protocol version that reads a table from the initiator's tasks.
node_old = cluster.add_instance(
    "node_old",
    main_configs=["configs/cluster.xml"],
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)

PARALLEL_REPLICAS_SETTINGS = {
    "enable_parallel_replicas": 1,
    "max_parallel_replicas": 3,
    "cluster_for_parallel_replicas": "parallel_replicas",
    "parallel_replicas_for_cluster_engines": 1,
}

QUERY = "SELECT sum(a), count() FROM t WHERE a > 10"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query(
            f"""
            INSERT INTO FUNCTION s3('http://minio1:9001/{cluster.minio_bucket}/data_127052/f_{{_partition_id}}.parquet',
                '{minio_access_key}', '{minio_secret_key}', 'Parquet')
            PARTITION BY a % 6 SELECT number AS a, toString(number) AS b FROM numbers(600)
            SETTINGS s3_truncate_on_insert = 1
            """
        )
        yield cluster
    finally:
        cluster.shutdown()


def create_table(nodes):
    for node in nodes:
        node.query(
            f"""
            CREATE OR REPLACE TABLE t (a UInt64, b String)
            ENGINE = S3('http://minio1:9001/{cluster.minio_bucket}/data_127052/*.parquet',
                '{minio_access_key}', '{minio_secret_key}', 'Parquet')
            """
        )


def secondary_queries(node, query_id):
    """The queries the node received for the read, finished or failed."""
    node.query("SYSTEM FLUSH LOGS query_log")
    return int(
        node.query(
            f"SELECT count() FROM system.query_log WHERE initial_query_id = '{query_id}' "
            "AND NOT is_initial_query AND type != 'QueryStart'"
        )
    )


def read_with_parallel_replicas():
    query_id = str(uuid.uuid4())
    result = node1.query(QUERY, settings=PARALLEL_REPLICAS_SETTINGS, query_id=query_id)
    return result, query_id


def test_old_replica_skipped(started_cluster):
    create_table([node1, node2, node_old])

    expected = node1.query(QUERY, settings={"enable_parallel_replicas": 0})
    result, query_id = read_with_parallel_replicas()

    assert result == expected
    assert secondary_queries(node1, query_id) == 1
    assert secondary_queries(node2, query_id) == 1
    assert secondary_queries(node_old, query_id) == 0


def test_replica_without_table_skipped(started_cluster):
    create_table([node1, node2, node_old])
    node2.query("DROP TABLE t SYNC")

    expected = node1.query(QUERY, settings={"enable_parallel_replicas": 0})
    result, query_id = read_with_parallel_replicas()

    assert result == expected
    assert secondary_queries(node1, query_id) == 1
    assert secondary_queries(node2, query_id) == 0
