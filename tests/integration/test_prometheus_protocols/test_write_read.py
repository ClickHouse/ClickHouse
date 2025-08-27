import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import *
import re
import requests
import time


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus_writer=True,
    with_prometheus_reader=True,
    handle_prometheus_remote_write=(9093, "/write"),
    handle_prometheus_remote_read=(9093, "/read"),
)


# Data are inserted via RemoteWrite protocol to ClickHouse,
# we need to wait a bit until we get some data.
def wait_for_data():
    start_time = time.monotonic()
    assert_eq_with_retry(
        node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
    )
    elapsed = time.monotonic() - start_time
    data_num_rows = int(node.query("SELECT count() FROM timeSeriesData(prometheus)"))
    tags_num_rows = int(node.query("SELECT count() FROM timeSeriesTags(prometheus)"))
    metrics_num_rows = int(
        node.query("SELECT count() FROM timeSeriesMetrics(prometheus)")
    )
    print(f"After waiting {elapsed} seconds got numbers of rows:")
    print(
        f"data: {data_num_rows} rows, tags: {tags_num_rows} rows, metrics: {metrics_num_rows} rows"
    )


# Executes a query in the "prometheus_reader" service. This service uses the RemoteRead protocol to get data from ClickHouse.
def execute_query_in_prometheus_reader(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_reader_ip,
        cluster.prometheus_reader_port,
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in the "prometheus_receiver" service. We send data to this service via the RemoteWrite protocol.
def execute_query_in_prometheus_writer(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_writer_ip,
        cluster.prometheus_writer_port,
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in both prometheus services - the results should be the same regardless of
# whether the data comes through ClickHouse or now.
def execute_query_in_prometheus(query, timestamp):
    r1 = execute_query_in_prometheus_reader(query, timestamp)
    r2 = execute_query_in_prometheus_writer(query, timestamp)
    assert r1 == r2
    return r1


# Executes a prometheus query in ClickHouse
def execute_query_in_clickhouse(query, timestamp):
    return node.query(
        f"SELECT * FROM prometheusQuery(prometheus, '{query}', {timestamp})"
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        yield cluster
    finally:
        cluster.shutdown()


def test_handle_normal_scrape():
    wait_for_data()
    query = "up"
    evaluation_time = time.time()
    result = execute_query_in_prometheus(query, evaluation_time)
    print(f"result={result}")
    pattern = '\{"resultType":\ "vector",\ "result":\ \[\{"metric":\ \{"__name__":\ "up",\ "instance":\ "localhost:9090",\ "job":\ "prometheus"},\ "value":\ \[[0-9]+(\.[0-9]*)?,\ "1"]}]}'
    assert re.match(pattern, result)
    chresult = execute_query_in_clickhouse(query, evaluation_time)
    print(f"chresult={chresult}")
    chpattern = "\[\('__name__','up'\),\('instance','localhost:9090'\),\('job','prometheus'\)]\t[^\t]*\t1\n"
    assert re.match(chpattern, chresult)


def test_remote_read_auth():
    wait_for_data()

    read_request = convert_read_request_to_protobuf(
        "^up$", time.time() - 300, time.time()
    )
    print(f"read_request={read_request}")

    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "read_auth_ok",
        read_request,
    )
    print(f"read_response = {read_response}")
    assert len(read_response.results) > 0
    assert len(read_response.results[0].timeseries) > 0
    assert len(read_response.results[0].timeseries[0].samples) > 0

    auth_fail_response = get_response_to_remote_read(
        node.ip_address,
        9093,
        "read_auth_fail",
        read_request,
    )
    assert auth_fail_response.status_code == requests.codes.forbidden
