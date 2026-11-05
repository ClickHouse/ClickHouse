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
        # System logs are disabled so that the new server does not create
        # rotated system log tables marked with the `table_readonly` setting,
        # which the older binary started via `restart_with_original_version`
        # would not know and would fail to attach.
        cluster.add_instance(
            "node",
            with_zookeeper=True,
            image="clickhouse/clickhouse-server",
            tag="25.10",
            main_configs=["configs/zz_disable_system_logs.xml"],
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

    # wait_for_active_replica only waits for is_readonly = 0, not for the freshly-joined
    # replica to finish fetching parts. SYSTEM SYNC REPLICA blocks until node2 has fetched
    # all parts; otherwise the count assertion below races with the background fetch and can
    # observe only the small VALUES part (1 row) before the 10000-row part arrives.
    node2.query("SYSTEM SYNC REPLICA test_alter_replay;")

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


def test_implicit_index_upgrade_alias_join_dropped(started_cluster):
    """25.10 stores the implicit indices over the ALIAS columns in Keeper and, after an
    ALTER, in the local definition. The newer server generates them as for a new table
    (none over the non-deterministic `joinGet`), and still loads the table and activates
    the replica once the `Join` table read by `a` is dropped."""
    node = started_cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS test_alias_join SYNC;")
    node.query("DROP TABLE IF EXISTS test_alias_join_src SYNC;")
    node.query(
        "CREATE TABLE test_alias_join_src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);"
    )
    node.query("INSERT INTO test_alias_join_src VALUES (1, 10);")
    node.query(
        """
        CREATE TABLE test_alias_join (
            key UInt64,
            a UInt64 ALIAS joinGet('default.test_alias_join_src', 'v', key),
            b UInt64 ALIAS key * 2
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_alias_join', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, add_minmax_index_for_string_columns=0;
        """
    )
    node.query("INSERT INTO test_alias_join VALUES (1), (2);")
    node.query("ALTER TABLE test_alias_join MODIFY COMMENT 'c';")
    definition = node.exec_in_container(
        ["cat", "/var/lib/clickhouse/metadata/default/test_alias_join.sql"]
    )
    assert "INDEX auto_minmax_index_a " in definition
    assert "INDEX auto_minmax_index_b " in definition

    node.restart_with_latest_version()
    wait_for_active_replica(node, "test_alias_join")
    assert node.query("SELECT key, a, b FROM test_alias_join ORDER BY key;") == "1\t10\t2\n2\t0\t4\n"
    assert (
        node.query(
            "SELECT name FROM system.data_skipping_indices WHERE table = 'test_alias_join' ORDER BY name;"
        )
        == "auto_minmax_index_b\nauto_minmax_index_key\n"
    )

    node.query("DROP TABLE test_alias_join_src SETTINGS check_table_dependencies = 0;")
    node.restart_clickhouse()

    wait_for_active_replica(node, "test_alias_join")
    assert node.query("SELECT key, b FROM test_alias_join ORDER BY key;") == "1\t2\n2\t4\n"
    node.query("INSERT INTO test_alias_join VALUES (3);")
    assert node.query("SELECT count() FROM test_alias_join;").strip() == "3"

    node.query("DROP TABLE test_alias_join SYNC;")
    node.restart_with_original_version()


def test_reserved_name_index_differing_from_implicit_is_kept(started_cluster):
    """An index with the implicit name but another granularity stays a regular index, in the
    local definition and in Keeper, so rewriting the definition keeps it, the replica stays
    active, and the marks its parts were written with stay readable."""
    node2 = started_cluster.instances["node2"]

    node2.query("DROP TABLE IF EXISTS test_reserved_name SYNC;")
    node2.query(
        """
        ATTACH TABLE test_reserved_name UUID '3d1e7c55-2f4e-4c8b-9a51-6f3d2e1a0b79' (
            key UInt64,
            x UInt64,
            INDEX auto_minmax_index_x x TYPE minmax GRANULARITY 8
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_reserved_name', 'r1')
        ORDER BY key
        SETTINGS add_minmax_index_for_numeric_columns=1, index_granularity=1;
        """
    )
    # The attached replica has no Keeper metadata yet; this writes it from the local definition.
    node2.query("SYSTEM RESTORE REPLICA test_reserved_name;")
    node2.query("INSERT INTO test_reserved_name SELECT number, number FROM numbers(64);")
    node2.query("ALTER TABLE test_reserved_name MODIFY COMMENT 'c';")
    node2.query("DETACH TABLE test_reserved_name;")
    node2.query("ATTACH TABLE test_reserved_name;")

    wait_for_active_replica(node2, "test_reserved_name")
    assert (
        node2.query(
            "SELECT granularity FROM system.data_skipping_indices WHERE table = 'test_reserved_name' AND name = 'auto_minmax_index_x';"
        )
        == "8\n"
    )
    assert (
        node2.query(
            "SELECT count() FROM test_reserved_name WHERE x = 50 SETTINGS force_data_skipping_indices = 'auto_minmax_index_x';"
        )
        == "1\n"
    )

    node2.query("DROP TABLE test_reserved_name SYNC;")
