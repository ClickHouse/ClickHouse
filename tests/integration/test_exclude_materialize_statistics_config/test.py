"""`exclude_materialize_statistics_on_merge` in the `merge_tree` section of the server configuration: the merges of
a table that does not set the setting itself use the configured value, and the server refuses to start with a value
that is not a list of column names, instead of letting the merges of every table with statistics fail.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/exclude_statistics.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/exclude_statistics.xml"


def exclude_statistics_config(value):
    return (
        "<clickhouse><merge_tree>"
        f"<exclude_materialize_statistics_on_merge>{value}</exclude_materialize_statistics_on_merge>"
        "</merge_tree></clickhouse>"
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_merges_use_config_value():
    node.query(
        """
        CREATE TABLE t_config (a UInt64, b UInt64, c UInt64)
        ENGINE = MergeTree ORDER BY a
        SETTINGS auto_statistics_types = 'basic', enable_block_number_column = 0, enable_block_offset_column = 0
        """
    )
    node.query("SYSTEM STOP MERGES t_config")
    for start in (0, 10):
        node.query(
            f"INSERT INTO t_config SELECT number, number, number FROM numbers({start}, 10)",
            settings={"materialize_statistics_on_insert": 0},
        )
    node.query("SYSTEM START MERGES t_config")
    node.query("OPTIMIZE TABLE t_config FINAL")

    has_statistics = node.query(
        """
        SELECT column, statistics != []
        FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_config' AND active
        ORDER BY column
        """
    )
    assert has_statistics == "a\t1\nb\t0\nc\t1\n"

    node.query("DROP TABLE t_config SYNC")


def test_invalid_config_value_prevents_startup():
    node.stop_clickhouse()
    node.replace_config(CONFIG_PATH, exclude_statistics_config("a b"))
    try:
        node.start_clickhouse(expected_to_fail=True)
        # `expected_to_fail` can return before the started process is even visible.
        node.wait_for_log_line(
            "Cannot parse the value 'a b' of setting 'exclude_materialize_statistics_on_merge' "
            "as a comma-separated list of column names"
        )
        node.wait_start_failed(60)
    finally:
        node.replace_config(CONFIG_PATH, exclude_statistics_config("b"))
        node.start_clickhouse()

    value = node.query(
        "SELECT value FROM system.merge_tree_settings WHERE name = 'exclude_materialize_statistics_on_merge'"
    )
    assert value == "b\n"
