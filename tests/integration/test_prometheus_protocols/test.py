import time
from http import HTTPStatus

import pytest
import requests
import snappy

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus=True,
    handle_prometheus_remote_write=True,
    handle_prometheus_remote_read=True,
)


def execute_query_on_prometheus_writer(query, timestamp):
    return execute_query_impl(
        cluster.prometheus_writer_ip,
        cluster.prometheus_writer_port,
        "/api/v1/query",
        query,
        timestamp,
    )


def execute_query_on_prometheus_reader(query, timestamp):
    return execute_query_impl(
        cluster.prometheus_reader_ip,
        cluster.prometheus_reader_port,
        "/api/v1/query",
        query,
        timestamp,
    )


def execute_query_impl(host, port, path, query, timestamp):
    if not path.startswith("/"):
        path += "/"
    url = f"http://{host}:{port}/{path.strip('/')}?query={query}&time={timestamp}"
    print(f"Requesting {url}")
    r = requests.get(url)
    print(f"Status code: {r.status_code} {HTTPStatus(r.status_code).phrase}")
    if r.status_code != requests.codes.ok:
        print(f"Response: {r.text}")
        raise Exception(f"Got unexpected status code {r.status_code}")
    return r.json()


def execute_remote_write_impl(host, port, path, body):
    if not path.startswith("/"):
        path += "/"
    url = f"http://{host}:{port}/{path.strip('/')}"
    print(f"Posting {url}")
    r = requests.post(
        url,
        data=snappy.compress(data=body),
        headers={
            "Content-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "User-Agent": requests.utils.default_user_agent(),
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        },
    )
    print(f"Status code: {r.status_code} {HTTPStatus(r.status_code).phrase}")
    if r.status_code != requests.codes.no_content:
        print(f"Response: {r.text}")
        raise Exception(f"Got unexpected status code {r.status_code}")
    return r.text


def show_query_result(query):
    evaluation_time = time.time()
    print(f"Evaluating query: {query}")
    print(f"Evaluation time: {evaluation_time}")
    result_from_writer = execute_query_on_prometheus_writer(query, evaluation_time)
    print(f"Result from prometheus_writer: {result_from_writer}")
    result_from_reader = execute_query_on_prometheus_reader(query, evaluation_time)
    print(f"Result from prometheus_reader: {result_from_reader}")


def compare_query(query):
    timeout = 60
    start_time = time.time()
    evaluation_time = start_time
    print(f"Evaluating query: {query}")
    print(f"Evaluation time: {evaluation_time}")
    while time.time() < start_time + timeout:
        result_from_writer = execute_query_on_prometheus_writer(query, evaluation_time)
        time.sleep(1)
        result_from_reader = execute_query_on_prometheus_reader(query, evaluation_time)
        print(f"Result from prometheus_writer: {result_from_writer}")
        print(f"Result from prometheus_reader: {result_from_reader}")
        if result_from_writer == result_from_reader:
            return
    raise Exception(
        f"Got different results from prometheus_writer and prometheus_reader"
    )


def compare_queries():
    compare_query("up")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS prometheus SYNC")
        node.query("DROP TABLE IF EXISTS original SYNC")
        node.query("DROP TABLE IF EXISTS mydata SYNC")
        node.query("DROP TABLE IF EXISTS mytable SYNC")
        node.query("DROP TABLE IF EXISTS mymetrics SYNC")


def test_default():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    compare_queries()


def test_tags_to_columns():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS tags_to_columns = {'job': 'job', 'instance': 'instance'}"
    )
    compare_queries()


def test_64bit_id():
    node.query("CREATE TABLE prometheus (id UInt64) ENGINE=TimeSeries")
    compare_queries()


def test_custom_id_algorithm():
    node.query(
        "CREATE TABLE prometheus (id FixedString(16) DEFAULT murmurHash3_128(metric_name, all_tags)) ENGINE=TimeSeries"
    )
    compare_queries()


def test_create_as_table():
    node.query("CREATE TABLE original ENGINE=TimeSeries")
    node.query("CREATE TABLE prometheus AS original")
    compare_queries()


def test_inner_engines():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "DATA ENGINE=MergeTree ORDER BY (id, timestamp) "
        "TAGS ENGINE=AggregatingMergeTree ORDER BY (metric_name, id) "
        "METRICS ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    compare_queries()


def test_external_tables():
    node.query("DROP TABLE IF EXISTS mydata")
    node.query("DROP TABLE IF EXISTS mytags")
    node.query("DROP TABLE IF EXISTS mymetrics")
    node.query("DROP TABLE IF EXISTS prometheus")

    node.query(
        "CREATE TABLE mydata (id UUID, timestamp DateTime64(3), value Float64) "
        "ENGINE=MergeTree ORDER BY (id, timestamp)"
    )
    node.query(
        "CREATE TABLE mytags ("
        "id UUID, "
        "metric_name LowCardinality(String), "
        "tags Map(LowCardinality(String), String), "
        "min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))), "
        "max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))) "
        "ENGINE=AggregatingMergeTree ORDER BY (metric_name, id)"
    )

    # FIXME: The table structure should be:
    # "CREATE TABLE mymetrics (metric_family_name String, type LowCardinality(String), unit LowCardinality(String), help String)"
    # Renamed it because of the bug and potential type mismatch.
    node.query(
        "CREATE TABLE mymetrics (metric_family_name String, type String, unit String, help String) "
        "ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
    )
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "DATA mydata TAGS mytags METRICS mymetrics"
    )
    compare_queries()


def test_read_auth():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    def get(path):
        headers = {
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
        }
        return requests.request(
            url=f"http://{node.ip_address}:{cluster.prometheus_remote_read_handler_port}{path}",
            method="GET",
            headers=headers,
        )

    auth_ok = get("/read_auth_ok")
    # FIXME: prometheus read handler requires proper payload with snappy
    # compression, but it will first try to authenticate and only after try to
    # interpret the payload, so those two lines below is a workaround to ensure
    # that the authentication works
    assert auth_ok.status_code == 500
    assert "DB::Exception: snappy uncomress failed" in auth_ok.text

    assert get("/read_auth_fail").status_code == 403


def test_remote_write_v1_status_code():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    # send a remote write v1 request created from the following object:
    # request := prompb.WriteRequest{
    # 	Metadata: []prompb.MetricMetadata{
    # 		{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: "time", Unit: "ms", Help: "Help Message"},
    # 	},
    # 	Timeseries: []prompb.TimeSeries{
    # 		{Labels: []prompb.Label{
    # 			{Name: "__name__", Value: "time"},
    # 			{Name: "region", Value: "us-east-1"},
    # 		}, Samples: []prompb.Sample{
    # 			{Value: 33.0, Timestamp: time.Now().Unix()},
    # 		}},
    # 	},
    # }
    # buf, _ := request.Marshal()
    # hex.EncodeToString(buf)
    body = bytes.fromhex(
        "0a380a100a085f5f6e616d655f5f120474696d650a130a06726567696f6e120975732d656173742d31120f09000000000080404010afdcb5bc061a1a0802120474696d65220c48656c70204d6573736167652a026d73"
    )
    execute_remote_write_impl(
        node.ip_address,
        cluster.prometheus_remote_write_handler_port,
        cluster.prometheus_remote_write_handler_path,
        body,
    )
