import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for i in (1, 2, 3, 4, 5, 6)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name):
    # create replicated tables
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    if cluster == "test_single_shard_multiple_replicas":
        nodes[0].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[1].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
        )
        nodes[2].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key)"
        )
        nodes[3].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r4') ORDER BY (key)"
        )
    elif cluster == "test_multiple_shards_multiple_replicas":
        # shard 1
        nodes[0].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[1].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
        )
        nodes[2].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key)"
        )
        # shard 2
        nodes[3].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[4].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r2') ORDER BY (key)"
        )
        nodes[5].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r3') ORDER BY (key)"
        )
    else:
        raise Exception(f"Unexpected cluster: {cluster}")

    # create distributed table
    nodes[0].query(f"DROP TABLE IF EXISTS {table_name}_d SYNC")
    nodes[0].query(
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
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(1000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(2000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT -number, -number FROM numbers(1000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT -number, -number FROM numbers(2000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(3)",
        settings={"distributed_foreground_insert": 1},
    )


@pytest.mark.parametrize(
    "cluster,max_parallel_replicas,prefer_localhost_replica",
    [
        pytest.param("test_single_shard_multiple_replicas", 2, 0),
        pytest.param("test_single_shard_multiple_replicas", 3, 0),
        pytest.param("test_single_shard_multiple_replicas", 4, 0),
        pytest.param("test_single_shard_multiple_replicas", 10, 0),
        pytest.param("test_single_shard_multiple_replicas", 2, 1),
        pytest.param("test_single_shard_multiple_replicas", 3, 1),
        pytest.param("test_single_shard_multiple_replicas", 4, 1),
        pytest.param("test_single_shard_multiple_replicas", 10, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 2, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 3, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 4, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 10, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 2, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 3, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 4, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 10, 1),
    ],
)
def test_parallel_replicas_over_distributed(
    start_cluster, cluster, max_parallel_replicas, prefer_localhost_replica
):
    table_name = "test_table"
    create_tables(cluster, table_name)

    node = nodes[0]
    expected_result = "6003\t-1999\t1999\t3\n"

    # sync all replicas to get consistent result
    node.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster} {table_name}")

    # parallel replicas
    assert (
        node.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 2,
                "prefer_localhost_replica": prefer_localhost_replica,
                "max_parallel_replicas": max_parallel_replicas,
            },
        )
        == expected_result
    )

    # w/o parallel replicas
    assert (
        node.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 0,
            },
        )
        == expected_result
    )


def test_parallel_replicas_over_distributed_on_replicated_database_cluster(
    start_cluster,
):
    # A `Replicated` database publishes its cluster under the database name, but assembles it from the
    # replica list in Keeper instead of reading it from `remote_servers`. Such a cluster carried no
    # name, and a distributed read ships `Cluster::getName` to the shards in
    # `cluster_for_parallel_replicas`, so the shard received an empty setting and refused to plan the
    # read at all: "Reading in parallel from replicas is enabled but cluster to execute query is not
    # provided" (CLUSTER_DOESNT_EXIST).
    database_name = "test_replicated_database_cluster"
    table_name = "test_replicated_database_cluster_table"
    group = nodes[0:3]

    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table_name}_d SYNC")
        node.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")

    for i, node in enumerate(group):
        node.query(
            f"CREATE DATABASE {database_name} "
            f"ENGINE = Replicated('/{database_name}/db', 'shard1', 'r{i}')"
        )

    group[0].query(
        f"CREATE TABLE {database_name}.{table_name} (key Int64) "
        f"ENGINE = ReplicatedMergeTree ORDER BY key"
    )
    group[0].query(
        f"INSERT INTO {database_name}.{table_name} SELECT number FROM numbers(1000)"
    )
    for node in group:
        node.query(f"SYSTEM SYNC REPLICA {database_name}.{table_name}")

    # The distributed table is kept outside the replicated database, so only the initiator has it. Its
    # cluster is the one the database publishes under its own name.
    nodes[0].query(
        f"CREATE TABLE {table_name}_d (key Int64) "
        f"Engine=Distributed({database_name}, {database_name}, {table_name}, key)"
    )

    # prefer_localhost_replica = 0 is mandatory for the same reason as in the test above: the shard has
    # to be read over the wire for the settings shipped with it to matter.
    settings = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 3,
        "prefer_localhost_replica": 0,
    }
    assert (
        nodes[0].query(
            f"SELECT count(), sum(key) FROM {table_name}_d", settings=settings
        )
        == "1000\t499500\n"
    )