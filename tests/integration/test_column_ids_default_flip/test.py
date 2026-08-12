import pytest

from helpers.cluster import ClickHouseCluster

CONFIG_PATH = "/etc/clickhouse-server/config.d/column_ids_default.xml"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/column_ids_default.xml"],
    user_configs=["configs/allow_experimental.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def has_mapping(table):
    """`column_ids.json` is what makes a table use column IDs, so ask the disk, not the settings."""
    uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()
    path = f"/var/lib/clickhouse/store/{uuid[:3]}/{uuid}/column_ids.json"
    return (
        node.exec_in_container(
            ["bash", "-c", f"test -f {path} && echo yes || echo no"]
        ).strip()
        == "yes"
    )


def test_flipping_the_default_leaves_older_tables_alone(started_cluster):
    """A table created before `serialization_info_version` is switched to `with_column_ids` server-wide
    keeps working after the switch, and a table created after it uses column IDs. The switch reaches
    every table that never named the setting, so it must not be read as "this table has a mapping"."""

    node.query(
        """
        CREATE TABLE t_legacy (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
        INSERT INTO t_legacy SELECT number, toString(number) FROM numbers(100);
        """
    )
    assert not has_mapping("t_legacy")
    assert node.query("SELECT count(), sum(a) FROM t_legacy") == "100\t4950\n"

    node.replace_in_config(CONFIG_PATH, "with_types", "with_column_ids")
    node.restart_clickhouse()

    assert (
        node.query(
            "SELECT value FROM system.merge_tree_settings WHERE name = 'serialization_info_version'"
        )
        == "with_column_ids\n"
    )
    # The switch is not an activation: nothing wrote a mapping for a table that never asked for one.
    assert not has_mapping("t_legacy")
    assert node.query("SELECT count(), sum(a) FROM t_legacy") == "100\t4950\n"

    node.query(
        """
        CREATE TABLE t_ids (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
        INSERT INTO t_ids SELECT number, toString(number) FROM numbers(100);
        """
    )
    assert has_mapping("t_ids")
    assert node.query("SELECT count(), sum(a) FROM t_ids") == "100\t4950\n"

    # Both tables take the same DDL and INSERT afterwards. Whether `t_legacy` acquires a mapping on
    # the way is deliberately not asserted -- that is the activation-trigger question, not this one.
    for table in ["t_legacy", "t_ids"]:
        node.query(
            f"""
            ALTER TABLE {table} RENAME COLUMN b TO c SETTINGS mutations_sync = 2;
            INSERT INTO {table} SELECT number, toString(number) FROM numbers(100, 50);
            ALTER TABLE {table} ADD COLUMN d UInt64 DEFAULT a * 2;
            OPTIMIZE TABLE {table} FINAL;
            """
        )
        assert node.query(f"SELECT count(), sum(a), sum(d) FROM {table}") == "150\t11175\t22350\n"
        assert node.query(f"SELECT c FROM {table} ORDER BY a LIMIT 1") == "0\n"

    node.restart_clickhouse()

    for table in ["t_legacy", "t_ids"]:
        assert node.query(f"SELECT count(), sum(a), sum(d) FROM {table}") == "150\t11175\t22350\n"
        assert node.query(f"SELECT c FROM {table} ORDER BY a LIMIT 1") == "0\n"
