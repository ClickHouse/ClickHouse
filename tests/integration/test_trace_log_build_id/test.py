import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

TEST_QUERY_ID = "test_trace_log_build_id_query_{}"
OLD_TEST_QUERY_ID = TEST_QUERY_ID.format("0")
NEW_TEST_QUERY_ID = TEST_QUERY_ID.format("1")
ACTIVE_TRACE_LOG_TABLE = "trace_log"
RENAMED_TRACE_LOG_TABLE = "trace_log_0"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
sanitizer_check_node = cluster.add_instance("sanitizer_check_node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_trace_log_build_id(started_cluster):
    # This test checks that build_id column of system_log.trace_log is non-empty, and gets renamed when binary version changes.
    # We make queries to create entries in trace_log, then restart with new version and verify if the old
    # trace_log table is renamed and a new trace_log table is created.

    if sanitizer_check_node.is_built_with_sanitizer():
        pytest.skip(
            "Sanitizers are skipped, because trace_log is disabled with sanitizers."
        )

    query_for_table_name = "EXISTS TABLE system.{table}"

    node.query(
        "SELECT sleep(2)",
        query_id=OLD_TEST_QUERY_ID,
    )
    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(query_for_table_name.format(table=ACTIVE_TRACE_LOG_TABLE)) == "1\n"
    )
    assert (
        node.query(query_for_table_name.format(table=RENAMED_TRACE_LOG_TABLE)) == "0\n"
    )

    node.restart_with_latest_version()

    query_for_test_query_id = """
        SELECT EXISTS
        (
            SELECT *
            FROM system.{table}
            WHERE query_id = \'{query_id}\'
        )
    """
    node.query(
        "SELECT sleep(2)",
        query_id=NEW_TEST_QUERY_ID,
    )
    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(
            query_for_test_query_id.format(
                table=ACTIVE_TRACE_LOG_TABLE, query_id=OLD_TEST_QUERY_ID
            )
        )
        == "0\n"
    )
    assert (
        node.query(
            query_for_test_query_id.format(
                table=ACTIVE_TRACE_LOG_TABLE, query_id=NEW_TEST_QUERY_ID
            )
        )
        == "1\n"
    )
    assert (
        node.query(
            query_for_test_query_id.format(
                table=RENAMED_TRACE_LOG_TABLE, query_id=OLD_TEST_QUERY_ID
            )
        )
        == "1\n"
    )
