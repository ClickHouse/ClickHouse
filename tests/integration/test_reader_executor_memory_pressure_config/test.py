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
    "WHERE name LIKE 'reader_executor_memory_pressure_level_%_pct' "
    "ORDER BY name"
)


def make_config(level_1, level_2, level_3):
    return (
        "<clickhouse>\n"
        f"    <reader_executor_memory_pressure_level_1_pct>{level_1}</reader_executor_memory_pressure_level_1_pct>\n"
        f"    <reader_executor_memory_pressure_level_2_pct>{level_2}</reader_executor_memory_pressure_level_2_pct>\n"
        f"    <reader_executor_memory_pressure_level_3_pct>{level_3}</reader_executor_memory_pressure_level_3_pct>\n"
        "</clickhouse>"
    )


def write_config(level_1, level_2, level_3):
    node.replace_config(CONFIG_PATH, make_config(level_1, level_2, level_3))


def thresholds_rows():
    return node.query(SELECT_THRESHOLDS)


def restore_good_config():
    write_config(40, 50, 60)
    node.query("SYSTEM RELOAD CONFIG")
    assert thresholds_rows() == (
        "reader_executor_memory_pressure_level_1_pct\t40\t1\tYes\n"
        "reader_executor_memory_pressure_level_2_pct\t50\t1\tYes\n"
        "reader_executor_memory_pressure_level_3_pct\t60\t1\tYes\n"
    )


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
    assert thresholds_rows() == (
        "reader_executor_memory_pressure_level_1_pct\t40\t1\tYes\n"
        "reader_executor_memory_pressure_level_2_pct\t50\t1\tYes\n"
        "reader_executor_memory_pressure_level_3_pct\t60\t1\tYes\n"
    )


def test_hot_reload(started_cluster):
    try:
        write_config(30, 35, 45)
        node.query("SYSTEM RELOAD CONFIG")
        assert thresholds_rows() == (
            "reader_executor_memory_pressure_level_1_pct\t30\t1\tYes\n"
            "reader_executor_memory_pressure_level_2_pct\t35\t1\tYes\n"
            "reader_executor_memory_pressure_level_3_pct\t45\t1\tYes\n"
        )
    finally:
        restore_good_config()


def test_invalid_rejected_on_reload(started_cluster):
    try:
        # Out-of-range value: level_1 = 150 > 100.
        write_config(150, 50, 60)
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "Memory pressure thresholds must" in error, error

        # The reload was rejected as a whole, so the live values are unchanged.
        assert thresholds_rows() == (
            "reader_executor_memory_pressure_level_1_pct\t40\t1\tYes\n"
            "reader_executor_memory_pressure_level_2_pct\t50\t1\tYes\n"
            "reader_executor_memory_pressure_level_3_pct\t60\t1\tYes\n"
        )

        # Out-of-order values: level_1 > level_2 > level_3.
        write_config(60, 50, 40)
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "Memory pressure thresholds must" in error, error

        assert thresholds_rows() == (
            "reader_executor_memory_pressure_level_1_pct\t40\t1\tYes\n"
            "reader_executor_memory_pressure_level_2_pct\t50\t1\tYes\n"
            "reader_executor_memory_pressure_level_3_pct\t60\t1\tYes\n"
        )
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
        assert thresholds_rows() == (
            "reader_executor_memory_pressure_level_1_pct\t40\t1\tYes\n"
            "reader_executor_memory_pressure_level_2_pct\t50\t1\tYes\n"
            "reader_executor_memory_pressure_level_3_pct\t60\t1\tYes\n"
        )
