# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node_default", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_query_log_engine(engine):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <engine>{engine}</engine>
                <partition_by remove='remove'/>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )


def set_query_log_comment(comment):
    set_query_log_engine(
        f"""ENGINE = MergeTree
                PARTITION BY (event_date)
                ORDER BY (event_time)
                TTL event_date + INTERVAL 14 DAY DELETE
                SETTINGS ttl_only_drop_parts=1
                COMMENT '{comment}'"""
    )


def test_system_logs_comment():
    set_query_log_comment("test_comment")
    node.restart_clickhouse()

    node.query("SELECT 1 SETTINGS log_comment = 'system_logs_comment_history'")
    node.query("SYSTEM FLUSH LOGS")

    comment = node.query(
        "SELECT comment FROM system.tables WHERE name = 'query_log' FORMAT TSVRaw"
    )
    assert (
        "\n\n.description\ntest_comment\n\n.description\n"
        "It is safe to truncate or drop this table at any time."
    ) in comment

    rotated_tables_before = int(
        node.query(
            "SELECT count() FROM system.tables "
            "WHERE database = 'system' AND match(name, '^query_log_[0-9]+$')"
        )
    )
    history_rows_before = int(
        node.query(
            "SELECT count() FROM system.query_log "
            "WHERE log_comment = 'system_logs_comment_history'"
        )
    )
    assert history_rows_before > 0

    set_query_log_comment("updated_comment")
    node.restart_clickhouse()
    node.query("SYSTEM FLUSH LOGS")

    updated_comment = node.query(
        "SELECT comment FROM system.tables WHERE name = 'query_log' FORMAT TSVRaw"
    )
    assert "\n\n.description\nupdated_comment\n\n.description\n" in updated_comment
    assert int(
        node.query(
            "SELECT count() FROM system.tables "
            "WHERE database = 'system' AND match(name, '^query_log_[0-9]+$')"
        )
    ) == rotated_tables_before
    assert int(
        node.query(
            "SELECT count() FROM system.query_log "
            "WHERE log_comment = 'system_logs_comment_history'"
        )
    ) >= history_rows_before


def test_bare_comment_is_rejected():
    node.stop_clickhouse()
    set_query_log_engine("ENGINE = MergeTree ORDER BY tuple() COMMENT")
    node.start_clickhouse(expected_to_fail=True)

    assert node.contains_in_log("Storage to create table for query_log")
    assert node.contains_in_log("string literal")

    set_query_log_comment("updated_comment")
    node.start_clickhouse()
