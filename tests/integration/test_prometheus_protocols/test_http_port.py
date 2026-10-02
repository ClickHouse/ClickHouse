import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    extract_error_from_http_api_response,
    get_response_to_http_api,
    receive_protobuf_from_remote_read,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

MAIN_HTTP_PORT = 8123
MAIN_HTTP_TIMESERIES_TABLE = "prometheus_http"

node = cluster.add_instance(
    "node",
    main_configs=["configs/http_port.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def send_to_clickhouse(time_series, write_path="/prometheus/api/v1/write"):
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(
        node.ip_address,
        MAIN_HTTP_PORT,
        write_path,
        protobuf,
    )


def read_metric_names(metric_name, start_timestamp, end_timestamp, read_path="/prometheus/api/v1/read"):
    read_request = convert_read_request_to_protobuf(
        "^{}$".format(metric_name), start_timestamp, end_timestamp
    )
    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        MAIN_HTTP_PORT,
        read_path,
        read_request,
    )
    metric_names = []
    for result in read_response.results:
        for time_series in result.timeseries:
            for label in time_series.labels:
                if label.name == "__name__":
                    metric_names.append(label.value)
    return metric_names


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query(
            f"CREATE TABLE {MAIN_HTTP_TIMESERIES_TABLE} ENGINE=TimeSeries"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_main_http_prefixed_remote_write():
    timestamp = 1_700_001_000.0
    metric_name = "main_http_prefixed_write_target"
    before = int(
        node.query(
            f"SELECT count() FROM timeSeriesData({MAIN_HTTP_TIMESERIES_TABLE})"
        ).strip()
    )

    send_to_clickhouse([({"__name__": metric_name}, {timestamp: 11.0})])

    after = int(
        node.query(
            f"SELECT count() FROM timeSeriesData({MAIN_HTTP_TIMESERIES_TABLE})"
        ).strip()
    )
    assert after > before


def test_main_http_prefixed_remote_read():
    timestamp = 1_700_001_100.0
    metric_name = "main_http_prefixed_read_target"

    send_to_clickhouse([({"__name__": metric_name}, {timestamp: 12.0})])

    metric_names = read_metric_names(metric_name, timestamp - 1, timestamp + 1)
    assert metric_name in metric_names


def test_main_http_prefixed_query_api():
    timestamp = 1_700_001_200.0
    metric_name = "main_http_prefixed_query_target"

    send_to_clickhouse([({"__name__": metric_name}, {timestamp: 13.0})])

    data = execute_query_via_http_api(
        node.ip_address,
        MAIN_HTTP_PORT,
        "/prometheus/api/v1/query",
        metric_name,
        timestamp=timestamp,
    )
    assert metric_name in data


def test_main_http_prefixed_query_range_api():
    timestamp = 1_700_001_250.0
    metric_name = "main_http_prefixed_query_range_target"

    send_to_clickhouse([({"__name__": metric_name}, {timestamp: 13.5})])

    data = execute_range_query_via_http_api(
        node.ip_address,
        MAIN_HTTP_PORT,
        "/prometheus/api/v1/query_range",
        metric_name,
        timestamp - 1,
        timestamp + 1,
        "1",
    )
    assert metric_name in data


def test_main_http_prefixed_label_values_api():
    timestamp = 1_700_001_275.0
    metric_name = "main_http_prefixed_label_values_target"
    label_value = "integration_test"

    send_to_clickhouse(
        [({"__name__": metric_name, "job": label_value}, {timestamp: 1.0})]
    )

    url = (
        f"http://{node.ip_address}:{MAIN_HTTP_PORT}"
        f"/prometheus/api/v1/label/job/values"
        f"?start={int(timestamp - 1)}"
        f"&end={int(timestamp + 1)}"
    )
    response = get_response_to_http_api(url)
    error = extract_error_from_http_api_response(response)
    assert "label values endpoint is not implemented" in error


def test_main_http_prefixed_and_bare_share_table():
    timestamp = 1_700_001_600.0
    prefixed_metric = "main_http_coexist_prefixed"
    bare_metric = "main_http_coexist_bare"

    send_to_clickhouse([({"__name__": prefixed_metric}, {timestamp: 17.0})])
    prefixed_read = read_metric_names(
        prefixed_metric,
        timestamp - 1,
        timestamp + 1,
        "/api/v1/read",
    )
    assert prefixed_metric in prefixed_read

    send_to_clickhouse(
        [({"__name__": bare_metric}, {timestamp + 1: 18.0})],
        "/api/v1/write",
    )
    bare_read = read_metric_names(bare_metric, timestamp, timestamp + 2)
    assert bare_metric in bare_read
