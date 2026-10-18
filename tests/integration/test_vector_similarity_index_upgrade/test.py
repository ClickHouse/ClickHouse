import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
# Accepts HNSW parameters that newer versions refuse to declare.
old_node = cluster.add_instance(
    "old_node",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag="25.10",
    with_installed_binary=True,
)
new_node = cluster.add_instance("new_node", with_zookeeper=True)

INDEX = "vector_similarity('hnsw', 'L2Distance', 2, 'f32', 1000, 128)"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def index_count_query(database, table):
    return f"SELECT count() FROM system.data_skipping_indices WHERE database = '{database}' AND table = '{table}'"


def index_count(database, table):
    return new_node.query(index_count_query(database, table)).strip()


def test_replicated_merge_tree_applies_index_added_by_older_replica():
    for name, node in [("old", old_node), ("new", new_node)]:
        node.query(
            f"CREATE TABLE t (id UInt64, vec Array(Float32)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', '{name}') ORDER BY id"
        )

    old_node.query(f"ALTER TABLE t ADD INDEX idx vec TYPE {INDEX}")
    new_node.query("SYSTEM SYNC REPLICA t", settings={"receive_timeout": 60})
    assert index_count("default", "t") == "1"

    old_node.query("ALTER TABLE t DROP INDEX idx")
    new_node.query("SYSTEM SYNC REPLICA t", settings={"receive_timeout": 60})
    assert index_count("default", "t") == "0"

    assert "must not be greater than 512" in new_node.query_and_get_error(
        f"ALTER TABLE t ADD INDEX idx vec TYPE {INDEX}"
    )


def test_replicated_database_applies_index_added_by_older_replica():
    for name, node in [("old", old_node), ("new", new_node)]:
        node.query(
            f"CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/databases/rdb', 'shard', '{name}')"
        )
    old_node.query(
        "CREATE TABLE rdb.m (id UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY id"
    )
    new_node.query("SYSTEM SYNC DATABASE REPLICA rdb")
    new_node.query("INSERT INTO rdb.m VALUES (1, [1, 2])")

    output = old_node.query(
        f"ALTER TABLE rdb.m ADD INDEX idx vec TYPE {INDEX}",
        settings={"distributed_ddl_task_timeout": 30},
    )
    statuses = [line.split("\t")[2] for line in output.strip().split("\n")]
    assert statuses == ["OK", "OK"], output
    # A replica reports OK at its Keeper commit, before it applies the ALTER to the table in memory.
    assert_eq_with_retry(new_node, index_count_query("rdb", "m"), "1")
    assert new_node.query("SELECT count() FROM rdb.m").strip() == "1"

    assert "must not be greater than 512" in new_node.query_and_get_error(
        f"ALTER TABLE rdb.m ADD INDEX idx2 vec TYPE {INDEX}"
    )
