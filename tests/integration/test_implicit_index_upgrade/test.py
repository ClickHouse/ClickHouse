import pytest
import time

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # 25.10 stored implicit indices in ZooKeeper metadata.
        # Newer versions only store explicit indices, so upgrading
        # requires backward-compatible metadata comparison.
        cluster.add_instance(
            "node",
            with_zookeeper=True,
            image="clickhouse/clickhouse-server",
            tag="25.10",
            with_installed_binary=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def wait_for_active_replica(node, table, timeout=30):
    for _ in range(timeout):
        is_readonly = node.query(
            f"SELECT is_readonly FROM system.replicas WHERE table = '{table}';"
        ).strip()
        if is_readonly == "0":
            return
        time.sleep(1)
    assert False, f"Replica for {table} is still in readonly mode after {timeout}s"


def test_implicit_index_upgrade_numeric(started_cluster):
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_numeric SYNC;")
    node.query(
        """
        CREATE TABLE test_numeric (
            key UInt64,
            value1 Int32,
            value2 Float64,
            label String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_numeric', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=0;
        """
    )

    node.query(
        "INSERT INTO test_numeric SELECT number, number % 100, number / 3.14, toString(number) FROM numbers(10000);"
    )

    old_indices = node.query(
        "SELECT name FROM system.data_skipping_indices WHERE table = 'test_numeric' ORDER BY name;"
    ).strip()
    assert "auto_minmax_index_key" in old_indices
    assert "auto_minmax_index_value1" in old_indices
    assert "auto_minmax_index_value2" in old_indices
    # String column should not have an implicit index with this setting
    assert "auto_minmax_index_label" not in old_indices

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM test_numeric;").strip() == "10000"
    wait_for_active_replica(node, "test_numeric")

    node.query("INSERT INTO test_numeric VALUES (99999, 1, 1.0, 'x');")
    assert node.query("SELECT count() FROM test_numeric;").strip() == "10001"

    node.query("DROP TABLE test_numeric SYNC;")
    node.restart_with_original_version()


def test_implicit_index_upgrade_string(started_cluster):
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_string SYNC;")
    node.query(
        """
        CREATE TABLE test_string (
            key UInt64,
            label String,
            tag String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_string', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=0, add_minmax_index_for_string_columns=1;
        """
    )

    node.query(
        "INSERT INTO test_string SELECT number, toString(number), 'tag' FROM numbers(10000);"
    )

    old_indices = node.query(
        "SELECT name FROM system.data_skipping_indices WHERE table = 'test_string' ORDER BY name;"
    ).strip()
    assert "auto_minmax_index_label" in old_indices
    assert "auto_minmax_index_tag" in old_indices
    # Numeric column should not have an implicit index with this setting
    assert "auto_minmax_index_key" not in old_indices

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM test_string;").strip() == "10000"
    wait_for_active_replica(node, "test_string")

    node.query("INSERT INTO test_string VALUES (99999, 'x', 'y');")
    assert node.query("SELECT count() FROM test_string;").strip() == "10001"

    node.query("DROP TABLE test_string SYNC;")
    node.restart_with_original_version()


def test_implicit_index_upgrade_mixed(started_cluster):
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_mixed SYNC;")
    node.query(
        """
        CREATE TABLE test_mixed (
            key UInt64,
            value Int32,
            label String,
            tag String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mixed', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=1;
        """
    )

    node.query(
        "INSERT INTO test_mixed SELECT number, number % 100, toString(number), 'tag' FROM numbers(10000);"
    )

    old_indices = node.query(
        "SELECT name FROM system.data_skipping_indices WHERE table = 'test_mixed' ORDER BY name;"
    ).strip()
    assert "auto_minmax_index_key" in old_indices
    assert "auto_minmax_index_value" in old_indices
    assert "auto_minmax_index_label" in old_indices
    assert "auto_minmax_index_tag" in old_indices

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM test_mixed;").strip() == "10000"
    wait_for_active_replica(node, "test_mixed")

    node.query("INSERT INTO test_mixed VALUES (99999, 1, 'x', 'y');")
    assert node.query("SELECT count() FROM test_mixed;").strip() == "10001"

    node.query("DROP TABLE test_mixed SYNC;")
    node.restart_with_original_version()


def test_implicit_index_upgrade_alter_replay(started_cluster):
    """Exercise `executeMetadataAlter`: node (25.10) creates a table with implicit
    indices, then ALTERs it (adding a column). node2 (latest) joins as a second
    replica and must replay the ALTER_METADATA entry whose metadata string was
    written by 25.10 and contains implicit indices."""
    node = started_cluster.instances["node"]
    node2 = started_cluster.instances["node2"]

    node.query("DROP TABLE IF EXISTS test_alter_replay SYNC;")
    node2.query("DROP TABLE IF EXISTS test_alter_replay SYNC;")

    # Create on 25.10 with implicit numeric indices.
    node.query(
        """
        CREATE TABLE test_alter_replay (
            key UInt64,
            value Int32,
            label String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_alter_replay', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=0;
        """
    )

    node.query(
        "INSERT INTO test_alter_replay SELECT number, number % 100, toString(number) FROM numbers(10000);"
    )

    # ALTER on 25.10: adds a column. The ALTER_METADATA log entry written to Keeper
    # contains skip_indices in old format (implicit indices included).
    node.query("ALTER TABLE test_alter_replay ADD COLUMN extra Float64 DEFAULT 0;")
    node.query("INSERT INTO test_alter_replay (key, value, label, extra) VALUES (99999, 1, 'x', 3.14);")

    # Upgrade node to latest so both replicas run the new code.
    node.restart_with_latest_version()
    wait_for_active_replica(node, "test_alter_replay")

    # node2 (latest) joins as second replica — it replays the full replication log
    # including the ALTER_METADATA entry written by 25.10.
    # Schema must match the post-ALTER state (includes `extra` column).
    node2.query(
        """
        CREATE TABLE test_alter_replay (
            key UInt64,
            value Int32,
            label String,
            extra Float64 DEFAULT 0
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_alter_replay', 'r2')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=0;
        """
    )

    wait_for_active_replica(node2, "test_alter_replay")

    # Verify node2 has the ALTER-added column and all data.
    assert node2.query("SELECT count() FROM test_alter_replay;").strip() == "10001"
    assert (
        node2.query(
            "SELECT extra FROM test_alter_replay WHERE key = 99999;"
        ).strip()
        == "3.14"
    )

    node.query("DROP TABLE test_alter_replay SYNC;")
    node2.query("DROP TABLE test_alter_replay SYNC;")
    node.restart_with_original_version()
