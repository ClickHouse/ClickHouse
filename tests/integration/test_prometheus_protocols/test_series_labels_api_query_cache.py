"""Regression test: the Prometheus metadata endpoints (/api/v1/series, /api/v1/labels,
/api/v1/label/<name>/values) must populate and hit the query result cache when the user enables
`use_query_cache`. The generated SQL is a plain deterministic scan of the tags table, so
`use_query_cache=1` alone is enough; the endpoints must call `finalizeWriteInQueryResultCache`
before finishing the query, otherwise the pending cache write is silently discarded when the
pulling executor is destroyed."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "memory_usage", "host": "server2", "datacenter": "us-west"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def get_json_from_api(path):
    url = f"http://{node.ip_address}:9093{path}"
    response = requests.get(url)
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


def get_query_cache_hits():
    return int(node.query("SELECT sum(value) FROM system.events WHERE event = 'QueryCacheHits'"))


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/series?match[]=cpu_usage",
        "/api/v1/labels",
        "/api/v1/label/host/values",
    ],
)
def test_metadata_endpoint_populates_and_hits_query_cache(path):
    node.query("SYSTEM DROP QUERY CACHE")
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0

    sep = "&" if "?" in path else "?"
    cached_path = f"{path}{sep}use_query_cache=1"

    first = get_json_from_api(cached_path)

    # The result must have been stored in the query result cache (the endpoint finalized the
    # pending cache write before finishing the query).
    assert int(node.query("SELECT count() FROM system.query_cache")) == 1

    # A second identical request must serve the result from the cache.
    hits_before = get_query_cache_hits()
    second = get_json_from_api(cached_path)
    assert second == first
    assert get_query_cache_hits() == hits_before + 1
