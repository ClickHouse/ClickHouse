import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# create only 2 nodes out of 3 nodes in cluster with 1 shard
# and out of 6 nodes in first shard in cluster with 2 shards
node1 = cluster.add_instance(
    "n1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "n2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name):
    # create replicated tables
    node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    node1.query(
        f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
    )
    node2.query(
        f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
    )

    # create distributed table
    node1.query(f"DROP TABLE IF EXISTS {table_name}_d SYNC")
    node1.query(
        f"""
            CREATE TABLE {table_name}_d AS {table_name}
            Engine=Distributed(
                {cluster},
                currentDatabase(),
                {table_name},
                key
            )
            """
    )

    # populate data
    node1.query(f"INSERT INTO {table_name} SELECT number, number FROM numbers(1000)")
    node2.query(f"INSERT INTO {table_name} SELECT -number, -number FROM numbers(1000)")
    node1.query(f"INSERT INTO {table_name} SELECT number, number FROM numbers(3)")
    # need to sync replicas to have consistent result
    node1.query(f"SYSTEM SYNC REPLICA {table_name}")
    node2.query(f"SYSTEM SYNC REPLICA {table_name}")


@pytest.mark.parametrize(
    "prefer_localhost_replica",
    [
        pytest.param(0),
        pytest.param(1),
    ],
)
def test_skip_unavailable_shards(start_cluster, prefer_localhost_replica):
    cluster = "test_multiple_shards_multiple_replicas"
    table_name = "test_table"
    create_tables(cluster, table_name)

    expected_result = f"2003\t-999\t999\t3\n"

    # w/o parallel replicas
    assert (
        node1.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d settings skip_unavailable_shards=1"
        )
        == expected_result
    )

    # parallel replicas
    assert (
        node1.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "prefer_localhost_replica": prefer_localhost_replica,
                "skip_unavailable_shards": 1,
                "connections_with_failover_max_tries": 0,  # just don't wait for unavailable replicas
            },
        )
        == expected_result
    )


@pytest.mark.parametrize(
    "prefer_localhost_replica",
    [
        pytest.param(0),
        pytest.param(1),
    ],
)
def test_error_on_unavailable_shards(start_cluster, prefer_localhost_replica):
    cluster = "test_multiple_shards_multiple_replicas"
    table_name = "test_table"
    create_tables(cluster, table_name)

    # w/o parallel replicas
    with pytest.raises(QueryRuntimeException):
        node1.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d settings skip_unavailable_shards=0"
        )

    # parallel replicas
    with pytest.raises(QueryRuntimeException):
        node1.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "prefer_localhost_replica": prefer_localhost_replica,
                "skip_unavailable_shards": 0,
            },
        )


@pytest.mark.parametrize(
    "skip_unavailable_shards",
    [
        pytest.param(0),
        pytest.param(1),
    ],
)
def test_no_unavailable_shards(start_cluster, skip_unavailable_shards):
    cluster = "test_single_shard_multiple_replicas"
    table_name = "test_table"
    create_tables(cluster, table_name)

    expected_result = f"2003\t-999\t999\t3\n"

    # w/o parallel replicas
    assert (
        node1.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d settings skip_unavailable_shards={skip_unavailable_shards}"
        )
        == expected_result
    )

    # parallel replicas
    assert (
        node1.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "prefer_localhost_replica": 0,
                "skip_unavailable_shards": skip_unavailable_shards,
            },
        )
        == expected_result
    )
