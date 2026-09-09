"""Enabling the union system log tables must not drop a user's table that sits on one of the names.

The union tables are proxies over a table function and hold no data, so the creation path replaces an
outdated one. `system.all_query_log` is also exactly the name a hand-rolled union of the rotated logs
takes, and such a table can be a `MergeTree` holding history - replacing that loses its data silently.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
# The section is added later, which is what an upgrade or a configuration change looks like.
node = cluster.add_instance("node", main_configs=[], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_user_table_on_a_union_name_survives(started_cluster):
    node.query("CREATE TABLE system.all_query_log (d Date, note String) ENGINE = MergeTree ORDER BY d")
    node.query("INSERT INTO system.all_query_log SELECT '2026-01-01', 'precious' FROM numbers(10)")
    assert node.query("SELECT count() FROM system.all_query_log WHERE note = 'precious'") == "10\n"

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/union_system_logs.xml"),
        "/etc/clickhouse-server/config.d/union_system_logs.xml",
    )
    node.restart_clickhouse()

    # The flush is what runs the union-table preparation for `query_log`.
    node.query("SELECT 42")
    node.query("SYSTEM FLUSH LOGS")

    assert node.query("SELECT count() FROM system.all_query_log WHERE note = 'precious'") == "10\n"
    assert (
        node.query("SELECT engine FROM system.tables WHERE database = 'system' AND name = 'all_query_log'")
        == "MergeTree\n"
    )
    assert node.contains_in_log("Not creating system.all_query_log")

    # A union name that nobody took is still claimed, so the feature itself keeps working.
    assert node.query("SELECT count() > 0 FROM system.all_part_log") == "1\n"

    node.query("DROP TABLE system.all_query_log SYNC")
