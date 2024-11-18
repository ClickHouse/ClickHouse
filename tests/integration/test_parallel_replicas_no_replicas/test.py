import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

initiator = cluster.add_instance(
    "initiator", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name):
    initiator.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    initiator.query(
        f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
    )

    # populate data
    initiator.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(1000)"
    )


@pytest.mark.parametrize("skip_unavailable_shards", [1, 0])
@pytest.mark.parametrize("max_parallel_replicas", [2, 3, 100])
def test_skip_all_replicas(
    start_cluster, skip_unavailable_shards, max_parallel_replicas
):
    cluster_name = "test_1_shard_3_unavaliable_replicas"
    table_name = "tt"
    create_tables(cluster_name, table_name)

    with pytest.raises(QueryRuntimeException):
        initiator.query(
            f"SELECT key, count() FROM {table_name}  GROUP BY key ORDER BY key",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": max_parallel_replicas,
                "cluster_for_parallel_replicas": cluster_name,
                "skip_unavailable_shards": skip_unavailable_shards,
                "parallel_replicas_local_plan": 0,
            },
        )
