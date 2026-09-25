# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/user_query_log.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/user_query_log.xml"

# A backtick-free substring of the guard error: `contains_in_log` greps inside a double-quoted shell
# string, where backticks would trigger command substitution.
GUARD_MESSAGE = "the query log table is always created in the"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _assert_startup_rejects_query_log_table_collision(database):
    # `system.user_query_log` is a virtual system table that shows the current user's query log records.
    # If `query_log.table` is `user_query_log`, the real query log table collides with it. The query log
    # table is always created in the `system` database - `SystemLog::createSystemLog` coerces any other
    # configured `query_log.database` back to `system` - so the collision happens regardless of the
    # configured database. The startup guard must reject it up front with a focused error rather than
    # letting the server die later on a generic "table already exists" exception.
    node.stop_clickhouse()
    node.replace_in_config(CONFIG_PATH, "<database>system</database>", f"<database>{database}</database>")
    node.replace_in_config(CONFIG_PATH, "<table>query_log</table>", "<table>user_query_log</table>")

    try:
        node.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
        assert node.get_process_pid("clickhouse") is None
        assert node.contains_in_log(GUARD_MESSAGE)
    finally:
        # Restore a valid configuration so the server (and module teardown) is healthy again.
        node.replace_in_config(CONFIG_PATH, "<table>user_query_log</table>", "<table>query_log</table>")
        node.replace_in_config(CONFIG_PATH, f"<database>{database}</database>", "<database>system</database>")
        node.start_clickhouse()
        assert node.get_process_pid("clickhouse") is not None
        node.rotate_logs()


def test_valid_config_starts(start_cluster):
    # A default query log table name starts normally and exposes the virtual `system.user_query_log`.
    assert node.get_process_pid("clickhouse") is not None
    assert (
        node.query(
            "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'user_query_log'"
        ).strip()
        == "SystemUserQueryLog"
    )


def test_startup_rejects_query_log_table_collision_system_database(start_cluster):
    _assert_startup_rejects_query_log_table_collision("system")


def test_startup_rejects_query_log_table_collision_non_system_database(start_cluster):
    _assert_startup_rejects_query_log_table_collision("foo")
