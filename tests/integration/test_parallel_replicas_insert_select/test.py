import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

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
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def execute_on_cluster(query):
    node1.query(query)
    node2.query(query)
    node3.query(query)


def create_tables(table_name, populate_count, skip_last_replica):
    execute_on_cluster(f"DROP TABLE IF EXISTS {table_name} SYNC")

    node1.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key) settings index_granularity=10"
    )
    node2.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key) settings index_granularity=10"
    )
    if not skip_last_replica:
        node3.query(
            f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key) settings index_granularity=10"
        )

    if populate_count == 0:
        return

    # populate data
    node1.query(
        f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers({populate_count})"
    )
    node2.query(f"SYSTEM SYNC REPLICA {table_name}")
    if not skip_last_replica:
        node3.query(f"SYSTEM SYNC REPLICA {table_name}")


@pytest.mark.parametrize(
    "cluster_name,max_parallel_replicas,local_pipeline,executed_queries",
    [
        pytest.param("test_1_shard_3_replicas", 2, False, 3),
        pytest.param("test_1_shard_3_replicas", 2, True, 2),
        pytest.param("test_1_shard_3_replicas", 3, False, 4),
        pytest.param("test_1_shard_3_replicas", 3, True, 3),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 3, False, 3),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 3, True, 2),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 2, False, 3),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 2, True, 2),
    ],
)
def test_insert_select(start_cluster, cluster_name, max_parallel_replicas, local_pipeline, executed_queries):
    populate_count = 1000000

    source_table = "t_source"
    create_tables(source_table, populate_count=populate_count, skip_last_replica=False)
    target_table = "t_target"
    create_tables(target_table, populate_count=0, skip_last_replica=False)

    query_id = str(uuid.uuid4())
    node1.query(
        f"INSERT INTO {target_table} SELECT * FROM {source_table}",
        settings={
            "parallel_distributed_insert_select": 2,
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": max_parallel_replicas,
            "cluster_for_parallel_replicas": cluster_name,
            "parallel_replicas_insert_select_local_pipeline": local_pipeline,
            "enable_analyzer": 1,
        },
        query_id=query_id
    )
    node1.query(f"SYSTEM SYNC REPLICA {target_table} LIGHTWEIGHT")
    assert (
        node1.query(
            f"select count() from {target_table}"
        )
        == f"{populate_count}\n"
    )
    assert (
        node1.query(
            f"select * from {target_table} order by key except select * from {source_table} order by key",
        )
        == ""
    )

    execute_on_cluster(f"SYSTEM FLUSH LOGS query_log")
    number_of_queries = node1.query(
            f"""SELECT count() FROM clusterAllReplicas({cluster_name}, system.query_log) WHERE current_database = currentDatabase() AND initial_query_id = '{query_id}' AND type = 'QueryFinish' AND query_kind = 'Insert'""",
        settings={"skip_unavailable_shards": 1},
    )

    if max_parallel_replicas < 3 and "unavailable" in cluster_name:
        # if max_parallel_replicas < number of nodes in cluster then nodes will be chosen randomly
        # and in case of cluster with unavailable node, the unavailable node can be chosen as well
        # so, in such case, number of executed queries will be one less
        assert(number_of_queries == f"{executed_queries}\n" or number_of_queries == f"{executed_queries-1}\n")
    else:
        assert(number_of_queries == f"{executed_queries}\n")


# TODO: Change protocol so we can check if all tables are present on a node
#       If not, then we can skip such nodes
#       Currently, we'll just fail

@pytest.mark.parametrize(
    "cluster_name,max_parallel_replicas,local_pipeline",
    [
        pytest.param("test_1_shard_3_replicas", 3, False),
        pytest.param("test_1_shard_3_replicas", 3, True),
    ],
)
def test_insert_select_no_table(start_cluster, cluster_name, max_parallel_replicas, local_pipeline):
    populate_count = 100

    source_table = "t_source"
    create_tables(source_table, populate_count=populate_count, skip_last_replica=True)
    target_table = "t_target"
    create_tables(target_table, populate_count=0, skip_last_replica=False)

    with pytest.raises(QueryRuntimeException) as e:
        node1.query(
            f"INSERT INTO {target_table} SELECT * FROM {source_table}",
            settings={
                "parallel_distributed_insert_select": 2,
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": max_parallel_replicas,
                "cluster_for_parallel_replicas": cluster_name,
                "parallel_replicas_insert_select_local_pipeline": local_pipeline,
                "enable_analyzer": 1,
            },
        )
    assert(e.value.returncode == 60) # UNKNOWN_TABLE


@pytest.mark.parametrize(
    "cluster_name,max_parallel_replicas,local_pipeline",
    [
        pytest.param("test_1_shard_3_replicas", 3, False),
        pytest.param("test_1_shard_3_replicas", 3, True),
    ],
)
def test_insert_select_no_target_table(start_cluster, cluster_name, max_parallel_replicas, local_pipeline):
    populate_count = 100

    source_table = "t_source"
    create_tables(source_table, populate_count=populate_count, skip_last_replica=False)
    target_table = "t_target"
    create_tables(target_table, populate_count=0, skip_last_replica=True)

    with pytest.raises(QueryRuntimeException) as e:
        node1.query(
            f"INSERT INTO {target_table} SELECT * FROM {source_table}",
            settings={
                "parallel_distributed_insert_select": 2,
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": max_parallel_replicas,
                "cluster_for_parallel_replicas": cluster_name,
                "parallel_replicas_insert_select_local_pipeline": local_pipeline,
                "enable_analyzer": 1,
            },
        )
    assert(e.value.returncode == 60) # UNKNOWN_TABLE


@pytest.mark.parametrize(
    "max_parallel_replicas",
    [
        pytest.param(2),
        pytest.param(3),
    ],
)
@pytest.mark.parametrize(
    "parallel_replicas_local_plan",
    [
        pytest.param(False),
        pytest.param(True),
    ]
)
def test_insert_select_limit(start_cluster, max_parallel_replicas, parallel_replicas_local_plan):
    populate_count = 1_000_000
    limit = 999_000
    cluster_name = "test_1_shard_3_replicas"

    source_table = "t_source"
    create_tables(source_table, populate_count=populate_count, skip_last_replica=False)
    target_table = "t_target"
    create_tables(target_table, populate_count=0, skip_last_replica=False)

    query_id = str(uuid.uuid4())
    node1.query(
        f"INSERT INTO {target_table} SELECT * FROM {source_table} LIMIT {limit}",
        settings={
            "parallel_distributed_insert_select": 2,
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": max_parallel_replicas,
            "cluster_for_parallel_replicas": cluster_name,
            "parallel_replicas_local_plan": parallel_replicas_local_plan,
            "enable_analyzer": 1,
        },
        query_id=query_id
    )
    node1.query(f"SYSTEM SYNC REPLICA {target_table} LIGHTWEIGHT")
    assert (
        node1.query(
            f"select count() from {target_table}"
        )
        == f"{limit}\n"
    )


@pytest.mark.parametrize(
    "max_parallel_replicas",
    [
        pytest.param(2),
        pytest.param(3),
    ],
)
@pytest.mark.parametrize(
    "parallel_replicas_local_plan",
    [
        pytest.param(False),
        pytest.param(True),
    ]
)
def test_insert_select_with_constant(start_cluster, max_parallel_replicas, parallel_replicas_local_plan):
    populate_count = 1_000_000
    cluster_name = "test_1_shard_3_replicas"

    source_table = "t_source"
    create_tables(source_table, populate_count=populate_count, skip_last_replica=False)
    target_table = "t_target"
    create_tables(target_table, populate_count=0, skip_last_replica=False)

    query_id = str(uuid.uuid4())
    node1.query(
        f"INSERT INTO {target_table} WITH 1 + 1 as two SELECT key + (select sum(number) from numbers(10) where number < two), value FROM {source_table}",
        settings={
            "parallel_distributed_insert_select": 2,
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": max_parallel_replicas,
            "cluster_for_parallel_replicas": cluster_name,
            "parallel_replicas_local_plan": parallel_replicas_local_plan,
            "enable_analyzer": 1,
        },
        query_id=query_id
    )
    node1.query(f"SYSTEM SYNC REPLICA {target_table} LIGHTWEIGHT")
    assert (
        node1.query(
            f"select count() from {target_table}"
        )
        == f"{populate_count}\n"
    )
