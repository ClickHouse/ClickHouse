import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": "1", "replica": "1"},
)
# Only node2 enables data_type_default_nullable, in the default profile its DDL worker also uses.
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/node2_users.xml"],
    with_zookeeper=True,
    macros={"shard": "2", "replica": "1"},
)

EXPECTED = "i\tInt32\nk\tInt32\nm\tNullable(Int32)\n"
EXPECTED_MODIFIED = "i\tInt32\nk\tInt64\nm\tNullable(Int64)\n"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def column_types(node, database):
    return node.query(
        f"SELECT name, type FROM system.columns WHERE database = '{database}' AND table = 't' ORDER BY position"
    )


def test_on_cluster():
    node1.query(
        "CREATE TABLE default.t ON CLUSTER test_cluster (i Int32) ENGINE = MergeTree ORDER BY i"
    )
    node1.query("ALTER TABLE default.t ON CLUSTER test_cluster ADD COLUMN k Int32")
    node2.query("ALTER TABLE default.t ON CLUSTER test_cluster ADD COLUMN m Int32")
    assert column_types(node1, "default") == EXPECTED
    assert column_types(node2, "default") == EXPECTED
    node1.query("ALTER TABLE default.t ON CLUSTER test_cluster MODIFY COLUMN k Int64")
    node2.query("ALTER TABLE default.t ON CLUSTER test_cluster MODIFY COLUMN m Int64")
    assert column_types(node1, "default") == EXPECTED_MODIFIED
    assert column_types(node2, "default") == EXPECTED_MODIFIED
    node1.query("DROP TABLE default.t ON CLUSTER test_cluster SYNC")


def test_replicated_database():
    for node in [node1, node2]:
        node.query(
            f"CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/databases/rdb', 'shard1', '{node.name}')"
        )
    node1.query("CREATE TABLE rdb.t (i Int32) ENGINE = MergeTree ORDER BY i")
    node1.query("ALTER TABLE rdb.t ADD COLUMN k Int32")
    node2.query("ALTER TABLE rdb.t ADD COLUMN m Int32")
    for node in [node1, node2]:
        node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert column_types(node1, "rdb") == EXPECTED
    assert column_types(node2, "rdb") == EXPECTED
    node1.query("ALTER TABLE rdb.t MODIFY COLUMN k Int64")
    node2.query("ALTER TABLE rdb.t MODIFY COLUMN m Int64")
    for node in [node1, node2]:
        node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    assert column_types(node1, "rdb") == EXPECTED_MODIFIED
    assert column_types(node2, "rdb") == EXPECTED_MODIFIED
    for node in [node1, node2]:
        node.query("DROP DATABASE rdb SYNC")
