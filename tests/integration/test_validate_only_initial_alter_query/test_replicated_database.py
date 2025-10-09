import pytest

from helpers.cluster import ClickHouseCluster
from .common import run_test

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    macros={"replica": 1, "shard": 1},
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": 2, "shard": 1},
)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_validate_only_initial_alter_query_replicated_database(started_cluster, engine):
    run_test(node1, node2, "Replicated('/clickhouse/databases/{database_name}', '{{shard}}', '{{replica}}')", engine)
