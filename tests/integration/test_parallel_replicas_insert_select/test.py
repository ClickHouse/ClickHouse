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


# `parallel_replicas_plan_based` must not change how the insert is distributed: the INSERT is shipped as
# a query, and a replica executing it always reads the query-tree-based way. The expected query counts are
# therefore the same for both values of the setting.
@pytest.mark.parametrize(
    "cluster_name,max_parallel_replicas,local_plan,executed_queries,plan_based",
    [
        pytest.param("test_1_shard_3_replicas", 2, False, 3, False),
        pytest.param("test_1_shard_3_replicas", 2, True, 2, False),
        pytest.param("test_1_shard_3_replicas", 3, False, 4, False),
        pytest.param("test_1_shard_3_replicas", 3, True, 3, False),
        pytest.param("test_1_shard_3_replicas", 3, False, 4, True),
        pytest.param("test_1_shard_3_replicas", 3, True, 3, True),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 3, False, 3, False),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 3, True, 2, False),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 2, False, 3, False),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 2, True, 2, False),
    ],
)
def test_insert_select(start_cluster, cluster_name, max_parallel_replicas, local_plan, executed_queries, plan_based):
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
            "parallel_replicas_local_plan": local_plan,
            "parallel_replicas_plan_based": plan_based,
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

    execute_on_cluster("SYSTEM FLUSH LOGS query_log")
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
    "cluster_name,max_parallel_replicas,local_plan",
    [
        pytest.param("test_1_shard_3_replicas", 3, False),
        pytest.param("test_1_shard_3_replicas", 3, True),
    ],
)
def test_insert_select_no_table(start_cluster, cluster_name, max_parallel_replicas, local_plan):
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
                "parallel_replicas_local_plan": local_plan,
                "enable_analyzer": 1,
            },
        )
    assert(e.value.returncode == 60) # UNKNOWN_TABLE


@pytest.mark.parametrize(
    "cluster_name,max_parallel_replicas,local_plan",
    [
        pytest.param("test_1_shard_3_replicas", 3, False),
        pytest.param("test_1_shard_3_replicas", 3, True),
    ],
)
def test_insert_select_no_target_table(start_cluster, cluster_name, max_parallel_replicas, local_plan):
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
                "parallel_replicas_local_plan": local_plan,
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
def test_insert_select_where(start_cluster, max_parallel_replicas, parallel_replicas_local_plan):
    populate_count = 1_000_000
    count = int(populate_count / 10)
    cluster_name = "test_1_shard_3_replicas"

    source_table = "t_source"
    create_tables(source_table, populate_count=populate_count, skip_last_replica=False)
    target_table = "t_target"
    create_tables(target_table, populate_count=0, skip_last_replica=False)

    query_id = str(uuid.uuid4())
    node1.query(
        f"INSERT INTO {target_table} SELECT * FROM {source_table} WHERE key % 10 = 0",
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
        == f"{count}\n"
    )

    # check that query executed in distributed way
    execute_on_cluster("SYSTEM FLUSH LOGS query_log")
    number_of_queries = node1.query(
            f"""SELECT count() FROM clusterAllReplicas({cluster_name}, system.query_log) WHERE current_database = currentDatabase() AND initial_query_id = '{query_id}' AND type = 'QueryFinish' AND query_kind = 'Insert'""",
        settings={"skip_unavailable_shards": 1},
    )
    assert (int(number_of_queries) > 1)


def create_replicated_database(db):
    for i, node in enumerate([node1, node2, node3], start=1):
        node.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', 'shard1', 'node{i}')"
        )


def sync_database(db):
    for node in [node1, node2, node3]:
        node.query(f"SYSTEM SYNC DATABASE REPLICA {db}")


def drop_database(db):
    for node in [node1, node2, node3]:
        node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def create_replicated_source(db):
    node1.query(f"CREATE TABLE {db}.src (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id")
    sync_database(db)
    node1.query(f"INSERT INTO {db}.src SELECT number FROM numbers(1000)")
    for node in [node2, node3]:
        node.query(f"SYSTEM SYNC REPLICA {db}.src")


def populate_settings(cluster_name, max_parallel_replicas, parallel_distributed_insert_select=2):
    return {
        "enable_parallel_replicas": 1,
        "max_parallel_replicas": max_parallel_replicas,
        "parallel_distributed_insert_select": parallel_distributed_insert_select,
        "parallel_replicas_local_plan": 0,
        "cluster_for_parallel_replicas": cluster_name,
    }


def query_per_node(query):
    return [node.query(query) for node in [node1, node2, node3]]


# Every replica of a Replicated database populates its own view, not the view of another replica.
def test_replicated_database_populate(start_cluster):
    db = f"db_{uuid.uuid4().hex}"
    try:
        create_replicated_database(db)
        create_replicated_source(db)
        node1.query(
            f"CREATE MATERIALIZED VIEW {db}.mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM {db}.src",
            settings=populate_settings("test_1_shard_2_replicas_1_unavailable", 2),
        )
        sync_database(db)
        assert query_per_node(f"SELECT count(), uniqExact(id), sum(id) FROM {db}.mv") == ["1000\t1000\t499500\n"] * 3
    finally:
        drop_database(db)


# The initiator populates its view while the other replicas have not created theirs yet.
def test_replicated_database_populate_before_other_replicas(start_cluster):
    db = f"db_{uuid.uuid4().hex}"
    try:
        create_replicated_database(db)
        create_replicated_source(db)
        for node in [node2, node3]:
            node.query("SYSTEM ENABLE FAILPOINT database_replicated_stop_entry_execution")
        node1.query(
            f"CREATE MATERIALIZED VIEW {db}.mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM {db}.src",
            settings={**populate_settings("test_1_shard_3_replicas", 3), "distributed_ddl_task_timeout": 0},
        )
        for node in [node2, node3]:
            node.query("SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution")
        sync_database(db)
        assert query_per_node(f"SELECT count(), uniqExact(id), sum(id) FROM {db}.mv") == ["1000\t1000\t499500\n"] * 3
    finally:
        for node in [node2, node3]:
            node.query("SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution")
        drop_database(db)


# `CREATE TABLE ... AS SELECT` fills the table of every replica the same way.
def test_replicated_database_create_as_select(start_cluster):
    db = f"db_{uuid.uuid4().hex}"
    try:
        create_replicated_database(db)
        create_replicated_source(db)
        node1.query(
            f"CREATE TABLE {db}.t ENGINE = Memory AS SELECT id FROM {db}.src",
            settings=populate_settings("test_1_shard_2_replicas_1_unavailable", 2),
        )
        sync_database(db)
        assert query_per_node(f"SELECT count(), uniqExact(id), sum(id) FROM {db}.t") == ["1000\t1000\t499500\n"] * 3
    finally:
        drop_database(db)


# The population reads the source of the replica executing it, also when the view's own `SELECT` enables parallel replicas.
def test_replicated_database_populate_reads_local_source(start_cluster):
    db = f"db_{uuid.uuid4().hex}"
    try:
        create_replicated_database(db)
        node1.query(f"CREATE TABLE {db}.src_local (id UInt64) ENGINE = MergeTree ORDER BY id")
        sync_database(db)
        for i, node in enumerate([node1, node2, node3]):
            node.query(f"INSERT INTO {db}.src_local SELECT {i * 1000} + number FROM numbers(1000)")
        node1.query(
            f"CREATE MATERIALIZED VIEW {db}.mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM {db}.src_local "
            "SETTINGS allow_experimental_parallel_reading_from_replicas = 1",
            settings={
                **populate_settings("test_1_shard_2_replicas_1_unavailable", 2, parallel_distributed_insert_select=0),
                "parallel_replicas_for_non_replicated_merge_tree": 1,
            },
        )
        sync_database(db)
        assert query_per_node(f"SELECT count(), min(id), max(id) FROM {db}.mv") == [
            "1000\t0\t999\n",
            "1000\t1000\t1999\n",
            "1000\t2000\t2999\n",
        ]
    finally:
        drop_database(db)
