#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/thresholds.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/thresholds.xml"

SELECT_THRESHOLDS = (
    "SELECT name, value, changed, changeable_without_restart "
    "FROM system.server_settings "
    "WHERE name LIKE 'reader_executor_memory_pressure_%_level_pct' "
    "ORDER BY name"
)


def expected_rows(elevated, high, critical):
    # `ORDER BY name` sorts the three settings alphabetically: critical, elevated, high.
    return (
        f"reader_executor_memory_pressure_critical_level_pct\t{critical}\t1\tYes\n"
        f"reader_executor_memory_pressure_elevated_level_pct\t{elevated}\t1\tYes\n"
        f"reader_executor_memory_pressure_high_level_pct\t{high}\t1\tYes\n"
    )


def make_config(elevated, high, critical):
    return (
        "<clickhouse>\n"
        f"    <reader_executor_memory_pressure_elevated_level_pct>{elevated}</reader_executor_memory_pressure_elevated_level_pct>\n"
        f"    <reader_executor_memory_pressure_high_level_pct>{high}</reader_executor_memory_pressure_high_level_pct>\n"
        f"    <reader_executor_memory_pressure_critical_level_pct>{critical}</reader_executor_memory_pressure_critical_level_pct>\n"
        "</clickhouse>"
    )


def write_config(elevated, high, critical):
    node.replace_config(CONFIG_PATH, make_config(elevated, high, critical))


def thresholds_rows():
    return node.query(SELECT_THRESHOLDS)


def restore_good_config():
    write_config(40, 50, 60)
    node.query("SYSTEM RELOAD CONFIG")
    assert thresholds_rows() == expected_rows(40, 50, 60)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_thresholds_taken_from_config(started_cluster):
    # Values come straight from configs/thresholds.xml, applied at startup.
    # changed=1 proves they differ from the 75/90/95 defaults (i.e. taken from
    # config); changeable_without_restart=Yes advertises the hot-reload capability.
    assert thresholds_rows() == expected_rows(40, 50, 60)


def test_hot_reload(started_cluster):
    try:
        write_config(30, 35, 45)
        node.query("SYSTEM RELOAD CONFIG")
        assert thresholds_rows() == expected_rows(30, 35, 45)
    finally:
        restore_good_config()


def test_invalid_rejected_on_reload(started_cluster):
    try:
        # Out-of-range value: elevated = 150 > 100.
        write_config(150, 50, 60)
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "Memory pressure thresholds must" in error, error

        # The reload was rejected as a whole, so the live values are unchanged.
        assert thresholds_rows() == expected_rows(40, 50, 60)

        # Out-of-order values: elevated > high > critical.
        write_config(60, 50, 40)
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "Memory pressure thresholds must" in error, error

        assert thresholds_rows() == expected_rows(40, 50, 60)
    finally:
        restore_good_config()


def test_invalid_rejected_at_startup(started_cluster):
    # The key assertion: a configured value out of range must abort startup,
    # which proves the value is pushed into the monitor and validated during the
    # initial config load (not only on SYSTEM RELOAD CONFIG).
    try:
        node.stop_clickhouse()
        write_config(150, 50, 60)
        node.start_clickhouse(expected_to_fail=True)
        # grep_in_log scans rotated .gz logs too (the failed-startup log gets
        # rotated away by the restore restart below). The "[0, 100]" part is a
        # regex character class for zgrep, so match only the literal prefix.
        assert node.grep_in_log("Memory pressure thresholds must be in")
    finally:
        write_config(40, 50, 60)
        node.start_clickhouse()
        assert thresholds_rows() == expected_rows(40, 50, 60)
