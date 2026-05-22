import pytest
import requests
from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import time

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_prometheus_keeper_metrics_only(start_cluster):
    keeper_only_async_metric_exists = int(
        node.query("SELECT count() > 0 FROM system.asynchronous_metrics WHERE metric = 'KeeperIsLeader'").strip()
    )
    assert keeper_only_async_metric_exists

    prometheus_handler_response = requests.get(
        f"http://{node.ip_address}:8001/metrics",
        timeout=5,
    )
    assert prometheus_handler_response.status_code == 200
    assert "ClickHouseAsyncMetrics_KeeperIsStandalone" in prometheus_handler_response.text
    assert "PolygonDictionaryThreads" not in prometheus_handler_response.text


def test_asynchronous_metric_log(start_cluster):
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM system.asynchronous_metric_log WHERE metric = 'KeeperIsLeader'",
        "1",
    )


def test_skip_alias_columns(start_cluster):
    node.query("SYSTEM FLUSH LOGS")
    # build_id is an ALIAS column on trace_log. With skip_alias_columns=true,
    # it should be absent from the table schema.
    error = node.query_and_get_error("SELECT build_id FROM system.trace_log LIMIT 0")
    assert "UNKNOWN_IDENTIFIER" in error


def test_system_keeper_changelogs(start_cluster):
    keeper_utils.wait_until_connected(cluster, node)

    # Force one entry into the active changelog so its size/last_entry_index are more deterministic.
    zk = keeper_utils.get_fake_zk(cluster, "node")
    try:
        zk.create("/test_system_keeper_changelogs", b"data")
    finally:
        zk.stop()
        zk.close()

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.keeper_changelogs WHERE active",
        "1",
    )

    row = node.query(
        "SELECT from_log_index, to_log_index, last_entry_index, entries, path, disk_name, "
        "size_bytes, toUnixTimestamp(modification_time), is_compressed, is_broken "
        "FROM system.keeper_changelogs WHERE active LIMIT 1 FORMAT TSV"
    ).strip().split("\t")

    (
        from_log_index,
        to_log_index,
        last_entry_index,
        entries,
        path,
        disk_name,
        size_bytes,
        modification_time,
        is_compressed,
        is_broken,
    ) = row

    assert int(from_log_index) >= 1
    assert int(entries) >= 1
    assert int(last_entry_index) == int(from_log_index) + int(entries) - 1
    assert path.startswith("changelog_") and "bin" in path
    assert disk_name != ""
    assert int(size_bytes) > 0
    assert int(modification_time) > 0
    assert int(is_broken) == 0
