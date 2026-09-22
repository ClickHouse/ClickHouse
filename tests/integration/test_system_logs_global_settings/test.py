# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/system_tables.xml"])


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def engine_full(table):
    return node.query(
        f"SELECT engine_full FROM system.tables WHERE database = 'system' AND name = '{table}'"
    )


def test_system_logs_global_settings():
    node.query("SELECT 1")
    node.query("SYSTEM FLUSH LOGS")

    # Global options apply to tables without their own configuration
    for table in ["part_log", "trace_log", "text_log"]:
        engine = engine_full(table)
        assert "TTL event_date + toIntervalDay(3)" in engine, engine
        assert "ttl_only_drop_parts = 1" in engine, engine

    # Per-table option wins, the rest is still taken from the global section
    engine = engine_full("query_log")
    assert "TTL event_date + toIntervalDay(5)" in engine, engine
    assert "ttl_only_drop_parts = 1" in engine, engine
