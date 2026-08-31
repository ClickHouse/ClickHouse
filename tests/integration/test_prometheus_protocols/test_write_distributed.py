import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_dist.xml",
        "configs/config.d/two_shards_dist.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

START_TIME = 1724112000
# Eight fixed hosts: the sharding hash is stable, so the split across the two shards is the same
# on every run, and with eight distinct keys both shards receive rows.
HOSTS = [f"h{i}" for i in range(8)]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE shard_0")
        node.query("CREATE DATABASE shard_1")
        node.query("CREATE TABLE shard_0.ts_local ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_local ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prom_dist AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_write_over_distributed():
    time_series = [
        ({"__name__": "dist_metric", "host": host}, {START_TIME + i: float(i)})
        for i, host in enumerate(HOSTS)
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/dist/write", protobuf)
    node.query("SYSTEM FLUSH DISTRIBUTED prom_dist")

    # Every sample lands exactly once across the shards, and the fixed hash split fills both.
    assert_eq_with_retry(
        node,
        "SELECT (SELECT count() FROM timeSeriesData(shard_0.ts_local))"
        " + (SELECT count() FROM timeSeriesData(shard_1.ts_local))",
        str(len(HOSTS)),
    )
    assert int(node.query("SELECT count() FROM timeSeriesData(shard_0.ts_local)")) > 0
    assert int(node.query("SELECT count() FROM timeSeriesData(shard_1.ts_local)")) > 0

    # Written data reads back through PromQL over the wrapper, in SQL and over HTTP.
    evaluation_time = START_TIME + len(HOSTS)
    sql_result = node.query(
        f"SELECT count() FROM prometheusQuery(prom_dist, 'dist_metric', {evaluation_time})"
    )
    assert int(sql_result) == len(HOSTS)
    http_result = execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", "count(dist_metric)", evaluation_time
    )
    assert f'"{len(HOSTS)}"' in http_result
