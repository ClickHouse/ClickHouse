"""A shard on an older server checks neither the target of its own insert nor the grants and row policies
of its timeSeriesSelector, so reads and remote write over it are refused until it is upgraded.
"""

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    get_error_from_query_endpoint,
    get_response_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_older_shard.xml",
        "configs/config.d/older_shard_dist.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)

START_TIME = 1724112000
OLDER_SERVER = "run an older server"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE default.ts ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE default.prom_older AS default.ts "
            "ENGINE = Distributed(older_shard_dist, 'default', ts)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_write_is_refused_while_a_shard_runs_an_older_server():
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/older/write",
        convert_time_series_to_protobuf(
            [({"__name__": "older_metric", "host": "h0"}, {START_TIME: 1.0})]
        ),
    )
    assert response.status_code >= 500, response.text
    assert OLDER_SERVER in response.text, response.text
    assert "old_node:9000" in response.text, response.text


@pytest.mark.parametrize(
    "table_function",
    [
        f"prometheusQuery(default.prom_older, 'older_metric', {START_TIME})",
        f"prometheusQueryRange(default.prom_older, 'older_metric', {START_TIME}, {START_TIME + 10}, 10)",
    ],
)
def test_table_functions_are_refused_while_a_shard_runs_an_older_server(table_function):
    error = node.query_and_get_error(f"SELECT count() FROM {table_function}")
    assert OLDER_SERVER in error, error


@pytest.mark.parametrize("endpoint", ["query", "query_range"])
def test_query_endpoints_are_refused_while_a_shard_runs_an_older_server(endpoint):
    error = get_error_from_query_endpoint(
        node.ip_address,
        9093,
        "/older_api",
        endpoint,
        "older_metric",
        START_TIME,
        START_TIME + 10,
        "10",
        {},
    )
    assert OLDER_SERVER in error, error
