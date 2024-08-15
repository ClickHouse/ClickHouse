import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1, "shard_bk": 3, "replica_bk": 2},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    macros={"shard": 2, "replica": 1, "shard_bk": 1, "replica_bk": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    macros={"shard": 3, "replica": 1, "shard_bk": 2, "replica_bk": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """
            CREATE DATABASE replica_1 ON CLUSTER cross_3shards_2replicas;
            CREATE DATABASE replica_2 ON CLUSTER cross_3shards_2replicas;

            CREATE TABLE replica_1.replicated_local
            ON CLUSTER cross_3shards_2replicas (part_key Date, id UInt32, shard_id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated', '{replica}')
            partition by part_key order by id;

            CREATE TABLE replica_1.replicated
            ON CLUSTER cross_3shards_2replicas as replica_1.replicated_local
            ENGINE = Distributed(cross_3shards_2replicas, '', replicated_local, shard_id);

            CREATE TABLE replica_2.replicated_local
            ON CLUSTER cross_3shards_2replicas (part_key Date, id UInt32, shard_id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard_bk}/replicated', '{replica_bk}')
            partition by part_key order by id;

            CREATE TABLE replica_2.replicated
            ON CLUSTER cross_3shards_2replicas as replica_2.replicated_local
            ENGINE = Distributed(cross_3shards_2replicas, '', replicated_local, shard_id);
            """
        )

        to_insert = """\
2017-06-16	10	0
2017-06-17	11	0
2017-06-16	20	1
2017-06-17	21	1
2017-06-16	30	2
2017-06-17	31	2
"""

        node1.query(
            "INSERT INTO replica_1.replicated FORMAT TSV",
            stdin=to_insert,
            settings={"insert_distributed_sync": 1},
        )
        yield cluster

    finally:
        # pass
        cluster.shutdown()


def test_alter_ddl(started_cluster):
    node1.query(
        "ALTER TABLE replica_1.replicated_local \
                ON CLUSTER cross_3shards_2replicas \
                UPDATE shard_id=shard_id+3 \
                WHERE part_key='2017-06-16'"
    )

    node1.query("SYSTEM SYNC REPLICA replica_2.replicated_local;", timeout=5)
    assert_eq_with_retry(
        node1,
        "SELECT count(*) FROM replica_2.replicated where shard_id >= 3 and part_key='2017-06-16'",
        "3",
    )

    node1.query(
        "ALTER TABLE replica_1.replicated_local  \
                ON CLUSTER cross_3shards_2replicas DELETE WHERE shard_id >=3;"
    )
    node1.query("SYSTEM SYNC REPLICA replica_2.replicated_local;", timeout=5)
    assert_eq_with_retry(
        node1, "SELECT count(*) FROM replica_2.replicated where shard_id >= 3", "0"
    )

    node2.query(
        "ALTER TABLE replica_1.replicated_local ON CLUSTER cross_3shards_2replicas DROP PARTITION '2017-06-17'"
    )

    node2.query("SYSTEM SYNC REPLICA replica_2.replicated_local;", timeout=5)
    assert_eq_with_retry(node1, "SELECT count(*) FROM replica_2.replicated", "0")


def test_atomic_database(started_cluster):
    node1.query(
        """DROP DATABASE IF EXISTS replica_1 ON CLUSTER cross_3shards_2replicas;
                   DROP DATABASE IF EXISTS replica_2 ON CLUSTER cross_3shards_2replicas;
                   CREATE DATABASE replica_1 ON CLUSTER cross_3shards_2replicas ENGINE=Atomic;
                   CREATE DATABASE replica_2 ON CLUSTER cross_3shards_2replicas ENGINE=Atomic;"""
    )

    assert "It's not supported for cross replication" in node1.query_and_get_error(
        "CREATE TABLE rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree ORDER BY n"
    )
    assert "It's not supported for cross replication" in node1.query_and_get_error(
        "CREATE TABLE replica_1.rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree ORDER BY n"
    )
    assert "It's not supported for cross replication" in node1.query_and_get_error(
        "CREATE TABLE rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree('/{shard}/{uuid}/', '{replica}') ORDER BY n"
    )
    assert "It's not supported for cross replication" in node1.query_and_get_error(
        "CREATE TABLE replica_2.rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree('/{shard}/{uuid}/', '{replica}') ORDER BY n"
    )
    assert (
        "For a distributed DDL on circular replicated cluster its table name must be qualified by database name"
        in node1.query_and_get_error(
            "CREATE TABLE rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree('/tables/{shard}/rmt/', '{replica}') ORDER BY n"
        )
    )

    node1.query(
        "CREATE TABLE replica_1.rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree('/tables/{shard}/rmt/', '{replica}') ORDER BY n"
    )
    node1.query(
        "CREATE TABLE replica_2.rmt ON CLUSTER cross_3shards_2replicas (n UInt64, s String) ENGINE=ReplicatedMergeTree('/tables/{shard_bk}/rmt/', '{replica_bk}') ORDER BY n"
    )

    assert (
        node1.query(
            "SELECT countDistinct(uuid) from remote('node1,node2,node3', 'system', 'databases') WHERE uuid != '00000000-0000-0000-0000-000000000000' AND name='replica_1'"
        )
        == "1\n"
    )
    assert (
        node1.query(
            "SELECT countDistinct(uuid) from remote('node1,node2,node3', 'system', 'tables') WHERE uuid != '00000000-0000-0000-0000-000000000000' AND name='rmt'"
        )
        == "2\n"
    )

    node1.query("INSERT INTO replica_1.rmt VALUES (1, 'test')")
    node2.query("SYSTEM SYNC REPLICA replica_2.rmt", timeout=5)
    assert_eq_with_retry(node2, "SELECT * FROM replica_2.rmt", "1\ttest")


def test_non_query_with_table_ddl(started_cluster):
    node1.query("CREATE USER A ON CLUSTER cross_3shards_2replicas")

    assert node1.query("SELECT 1", user="A") == "1\n"
    assert node2.query("SELECT 1", user="A") == "1\n"

    node2.query("DROP USER A ON CLUSTER cross_3shards_2replicas")
