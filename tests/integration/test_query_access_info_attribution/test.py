import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

CONFIG_PATH = "/etc/clickhouse-server/config.d/query_access_info_attribution.xml"
CONFIG_ON = """<clickhouse>
    <query_log><enable_query_access_info_attribution>1</enable_query_access_info_attribution></query_log>
</clickhouse>"""
CONFIG_OFF = """<clickhouse>
    <query_log><enable_query_access_info_attribution>0</enable_query_access_info_attribution></query_log>
</clickhouse>"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query_view(comment, analyzer_inline_views):
    result = node.query(
        "SELECT sum(id) FROM default.qai_attribution_view "
        f"SETTINGS analyzer_inline_views = {analyzer_inline_views}, log_queries = 1, log_comment = '{comment}'",
        user="qai_attribution_reader",
    )
    assert result.strip() == "1"
    node.query("SYSTEM FLUSH LOGS")


def query_materialized_view(comment, view_name):
    result = node.query(
        f"SELECT sum(id) FROM default.{view_name} SETTINGS log_queries = 1, log_comment = '{comment}'",
        user="qai_attribution_reader",
    )
    assert result.strip() == "1"
    node.query("SYSTEM FLUSH LOGS")


def query_log_row(comment, private_name):
    return node.query(
        "SELECT count(), countIf(query_access_info_is_hidden = 0), "
        "countIf(query_access_info_is_hidden = 1), "
        "countIf(arrayExists(token -> position(token, '" + private_name + "') > 0, "
        "arrayConcat(databases, tables, columns, partitions, projections, views, used_row_policies))) "
        "FROM system.query_log WHERE initial_query_id = query_id "
        "AND initial_user = 'qai_attribution_reader' AND type = 'QueryFinish' "
        f"AND log_comment = '{comment}'"
    ).strip()


def user_query_log_count(comment):
    return node.query(
        "SELECT count() FROM system.user_query_log WHERE initial_query_id = query_id "
        f"AND type = 'QueryFinish' AND log_comment = '{comment}'",
        user="qai_attribution_reader",
    ).strip()


def assert_access_denied(table_name):
    with pytest.raises(QueryRuntimeException) as exc:
        node.query(f"SELECT * FROM default.{table_name}", user="qai_attribution_reader")
    assert "ACCESS_DENIED" in str(exc.value)


def assert_explicit_mv_read_is_safe(comment, target_name):
    row_count, visible_rows, hidden_rows, private_metadata_rows = map(
        int, query_log_row(comment, target_name).split("\t")
    )
    assert row_count == 1
    assert visible_rows + hidden_rows == 1
    # A surviving private target reference must keep the query out of the reader's own log.
    assert private_metadata_rows == 0 or hidden_rows == 1
    assert user_query_log_count(comment) == ("0" if hidden_rows else "1")


def test_opt_in_default_and_persisted_privacy_mark():
    node.query(
        "CREATE TABLE default.qai_attribution_source (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    node.query("INSERT INTO default.qai_attribution_source VALUES (1)")
    node.query(
        "CREATE VIEW default.qai_attribution_view DEFINER = CURRENT_USER SQL SECURITY DEFINER "
        "AS SELECT id FROM default.qai_attribution_source"
    )

    for suffix in ("definer", "none", "legacy"):
        node.query(
            f"CREATE TABLE default.qai_attribution_{suffix}_source (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            f"CREATE TABLE default.qai_attribution_{suffix}_target (id UInt64) ENGINE = MergeTree ORDER BY id"
        )

    node.query(
        "CREATE MATERIALIZED VIEW default.qai_attribution_definer_mv "
        "TO default.qai_attribution_definer_target DEFINER = CURRENT_USER SQL SECURITY DEFINER "
        "AS SELECT id FROM default.qai_attribution_definer_source"
    )
    node.query(
        "CREATE MATERIALIZED VIEW default.qai_attribution_none_mv "
        "TO default.qai_attribution_none_target DEFINER = CURRENT_USER SQL SECURITY NONE "
        "AS SELECT id FROM default.qai_attribution_none_source"
    )
    node.query(
        "CREATE MATERIALIZED VIEW default.qai_attribution_legacy_mv "
        "TO default.qai_attribution_legacy_target AS SELECT id FROM default.qai_attribution_legacy_source"
    )
    for suffix in ("definer", "none", "legacy"):
        node.query(f"INSERT INTO default.qai_attribution_{suffix}_source VALUES (1)")

    node.query("CREATE USER qai_attribution_reader IDENTIFIED WITH no_password")
    node.query("GRANT SELECT ON default.qai_attribution_view TO qai_attribution_reader")
    for suffix in ("definer", "none", "legacy"):
        node.query(f"GRANT SELECT ON default.qai_attribution_{suffix}_source TO qai_attribution_reader")
        node.query(f"GRANT SELECT ON default.qai_attribution_{suffix}_mv TO qai_attribution_reader")

    assert_access_denied("qai_attribution_source")
    for suffix in ("definer", "none", "legacy"):
        assert_access_denied(f"qai_attribution_{suffix}_target")

    # An absent SQL SECURITY clause remains the legacy, protected MV-read path with the option OFF.
    assert node.query(
        "SELECT count() FROM system.tables WHERE database = 'default' "
        "AND name = 'qai_attribution_legacy_mv' "
        "AND positionCaseInsensitive(create_table_query, 'SQL SECURITY') = 0 "
        "AND positionCaseInsensitive(create_table_query, 'DEFINER') = 0"
    ).strip() == "1"
    query_materialized_view("qai_attribution_legacy_mv_off", "qai_attribution_legacy_mv")
    assert query_log_row(
        "qai_attribution_legacy_mv_off", "qai_attribution_legacy_target"
    ) == "1\t0\t1\t1"
    assert user_query_log_count("qai_attribution_legacy_mv_off") == "0"

    # The changed explicit-security MV path must either omit its private target details or mark the row hidden.
    for suffix in ("definer", "none"):
        comment = f"qai_attribution_{suffix}_mv_off"
        query_materialized_view(comment, f"qai_attribution_{suffix}_mv")
        assert_explicit_mv_read_is_safe(comment, f"qai_attribution_{suffix}_target")

    # The default OFF setting keeps both analyzer paths for an eligible trivial view caller-visible.
    for inline_views in (0, 1):
        comment = f"qai_attribution_view_off_initial_{inline_views}"
        query_view(comment, inline_views)
        assert query_log_row(comment, "qai_attribution_source") == "1\t1\t0\t0"
        assert user_query_log_count(comment) == "1"

    node.replace_config(CONFIG_PATH, CONFIG_ON)
    node.restart_clickhouse()
    query_view("qai_attribution_on", 1)
    assert query_log_row("qai_attribution_on", "qai_attribution_source") == "1\t0\t1\t1"
    assert user_query_log_count("qai_attribution_on") == "0"

    # The persisted protected row stays hidden after the server option is switched OFF and restarted.
    node.replace_config(CONFIG_PATH, CONFIG_OFF)
    node.restart_clickhouse()
    assert query_log_row("qai_attribution_on", "qai_attribution_source") == "1\t0\t1\t1"
    assert user_query_log_count("qai_attribution_on") == "0"
    assert node.query("SELECT 1", user="qai_attribution_reader").strip() == "1"
    assert_access_denied("qai_attribution_source")

    # OFF remains caller-visible for new queries in both analyzer paths after restart.
    for inline_views in (0, 1):
        comment = f"qai_attribution_view_off_after_restart_{inline_views}"
        query_view(comment, inline_views)
        assert query_log_row(comment, "qai_attribution_source") == "1\t1\t0\t0"
        assert user_query_log_count(comment) == "1"
