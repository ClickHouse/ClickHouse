import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import tsv_close_to
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_reader=True,
    with_prometheus_receiver=True,
)


# Sends data [ ({'label_name1': 'label_value1], ...}, {timestamp1: value1, ...} ), ... ]
# to the "protobuf_receiver" service and also to ClickHouse via the RemoteWrite protocol.
def send_data(time_series):
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(
        cluster.prometheus_receiver_ip,
        cluster.prometheus_receiver_port,
        "api/v1/write",
        protobuf,
    )
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


# Executes a query in the "prometheus_reader" service. This service uses the RemoteRead protocol to get data from ClickHouse.
def execute_query_in_prometheus_reader(query, timestamp=None, expect_error=False):
    return execute_query_via_http_api(
        cluster.prometheus_reader_ip,
        cluster.prometheus_reader_port,
        "/api/v1/query",
        query,
        timestamp=timestamp,
        expect_error=expect_error,
    )


# Executes a query in the "prometheus_receiver" service. We sent data to this service earlier via the RemoteWrite protocol.
def execute_query_in_prometheus_receiver(query, timestamp, expect_error=False):
    return execute_query_via_http_api(
        cluster.prometheus_receiver_ip,
        cluster.prometheus_receiver_port,
        "/api/v1/query",
        query,
        timestamp=timestamp,
        expect_error=expect_error,
    )


# Executes a query in both prometheus services - results should be the same.
def execute_query_in_prometheus(query, timestamp, expect_error=False):
    r1 = execute_query_in_prometheus_reader(query, timestamp, expect_error=expect_error)
    r2 = execute_query_in_prometheus_receiver(
        query, timestamp, expect_error=expect_error
    )
    assert r1 == r2
    return r1


# Executes a prometheus query in ClickHouse via HTTP API
def execute_query_in_clickhouse_http_api(query, timestamp, expect_error=False):
    return execute_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query",
        query,
        timestamp=timestamp,
        expect_error=expect_error,
    )


# Executes a prometheus query in ClickHouse via SQL query
def execute_query_in_clickhouse_sql(query, timestamp, expect_error=False):
    quoted_query = "'" + query.replace("'", "''") + "'"
    sql_query = (
        f"SELECT * FROM prometheusQuery(prometheus, {quoted_query}, {timestamp})"
    )
    if expect_error:
        return node.query_and_get_error(sql_query)
    return node.query(sql_query)


# Executes a range query in both prometheus services.
def execute_range_query_in_prometheus(query, start_time, end_time, step):
    r1 = execute_range_query_via_http_api(
        cluster.prometheus_reader_ip,
        cluster.prometheus_reader_port,
        "/api/v1/query_range",
        query,
        start_time,
        end_time,
        step,
    )
    r2 = execute_range_query_via_http_api(
        cluster.prometheus_receiver_ip,
        cluster.prometheus_receiver_port,
        "/api/v1/query_range",
        query,
        start_time,
        end_time,
        step,
    )
    assert r1 == r2
    return r1


# Executes a range query in ClickHouse via HTTP API
def execute_range_query_in_clickhouse_http_api(query, start_time, end_time, step):
    return execute_range_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query_range",
        query,
        start_time,
        end_time,
        step,
    )


# Executes a range query in ClickHouse via SQL query
def execute_range_query_in_clickhouse_sql(query, start_time, end_time, step):
    return node.query(
        f"SELECT * FROM prometheusQueryRange(prometheus, '{query}', {start_time}, {end_time}, {step})"
    )


# Sends all test data to the "protobuf_receiver" service and also to ClickHouse via the RemoteWrite protocol.
def send_test_data():
    send_data([({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})])

    send_data(
        [
            (
                {"__name__": "test"},
                {
                    110: 1,
                    120: 1,
                    130: 3,
                    140: 4,
                    190: 5,
                    200: 5,
                    210: 8,
                    220: 12,
                    230: 13,
                },
            )
        ]
    )

    send_data(
        [
            (
                {"__name__": "timestamps", "job": "test"},
                {
                    110: 1764498605,
                    120: 1765035045,
                },
            ),
        ]
    )

    send_data(
        [
            (
                {"__name__": "deltas", "job": "test"},
                {
                    100: -2,
                    200: -1,
                    300: -0.5,
                    400: 0,
                    500: 0.5,
                    600: 1,
                    700: 2,
                },
            ),
        ]
    )

    send_data(
        [
            (
                {"__name__": "http_errors", "http_code": "401"},
                {
                    150: 0,
                    200: 4,
                },
            ),
            (
                {"__name__": "http_errors", "http_code": "404"},
                {
                    110: 1,
                    120: 5,
                },
            ),
            (
                {"__name__": "download_failures", "http_code": "404"},
                {
                    130: 0,
                    150: 1,
                },
            ),
        ]
    )

    send_data(
        [
            (
                {"__name__": "foo", "shape": "square", "size": "s"},
                {110: 4, 130: 40},
            ),
            (
                {"__name__": "foo", "shape": "triangle", "size": "m"},
                {110: 8, 120: 80},
            ),
            (
                {"__name__": "foo", "shape": "circle", "size": "l"},
                {110: 16, 130: 16, 150: 16},
            ),

            (
                {"__name__": "bar", "shape": "circle", "size": "l"},
                {110: 10, 120: 16, 130: 50, 150: 1000},
            ),
            (
                {"__name__": "bar", "shape": "square", "size": "s"},
                {110: 3, 120: 40, 140: 700},
            ),
            (
                {"__name__": "bar", "shape": "triangle", "size": "xl"},
                {110: 8, 150: 30},
            ),
            (
                {"__name__": "bar", "shape": "rectangle", "size": "l"},
                {110: 9, 130: 90},
            ),
        ]
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        yield cluster
    finally:
        cluster.shutdown()


# Evaluates the same query in Prometheus and in ClickHouse and compare the results.
def do_query_test(
    query,
    timestamp,
    result,
    chresult,
    clickhouse_http_api_result_is_same_as_prometheus=True,
    eps=0,
):
    assert execute_query_in_prometheus(query, timestamp) == result

    actual_chresult = execute_query_in_clickhouse_sql(query, timestamp)
    assert tsv_close_to(
        actual_chresult, chresult, eps=eps
    ), f"actual result: {actual_chresult}, expected: {chresult}"

    actual_result_from_http_api = execute_query_in_clickhouse_http_api(query, timestamp)
    assert (
        http_api_response_close_to(actual_result_from_http_api, result, eps=eps)
        == clickhouse_http_api_result_is_same_as_prometheus
    ), f"actual_result_from_http_api: {actual_result_from_http_api}, expected: {result}"


def do_query_test_expect_error(
    query,
    timestamp,
    expected_error,
    expected_cherror,
):
    assert expected_error in execute_query_in_prometheus_reader(
        query, timestamp, expect_error=True
    )
    assert expected_error in execute_query_in_prometheus_receiver(
        query, timestamp, expect_error=True
    )
    assert expected_cherror in execute_query_in_clickhouse_sql(
        query, timestamp, expect_error=True
    )
    assert expected_cherror in execute_query_in_clickhouse_http_api(
        query, timestamp, expect_error=True
    )


# Evaluates the same range query in Prometheus and in ClickHouse and compare the results.
def do_range_query_test(
    query,
    start_time,
    end_time,
    step,
    result,
    chresult,
    clickhouse_http_api_result_is_same_as_prometheus=True,
    eps=0,
):
    assert (
        execute_range_query_in_prometheus(query, start_time, end_time, step) == result
    )

    actual_chresult = execute_range_query_in_clickhouse_sql(
        query, start_time, end_time, step
    )
    assert tsv_close_to(
        actual_chresult, chresult, eps=eps
    ), f"actual result: {actual_chresult}, expected: {chresult}"

    actual_result_from_http_api = execute_range_query_in_clickhouse_http_api(
        query, start_time, end_time, step
    )
    assert (
        http_api_response_close_to(actual_result_from_http_api, result, eps=eps)
        == clickhouse_http_api_result_is_same_as_prometheus
    ), f"actual_result_from_http_api: {actual_result_from_http_api}, expected: {result}"


def test_up():
    do_query_test(
        "up",
        1753176757.89,
        '{"resultType": "vector", "result": [{"metric": {"__name__": "up", "job": "prometheus"}, "value": [1753176757.89, "1"]}]}',
        [["[('__name__','up'),('job','prometheus')]", "2025-07-22 09:32:37.890", "1"]],
    )


def test_range_selectors():
    do_query_test(
        "test[30s]",
        129,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[110, "1"], [120, "1"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1)]",
            ]
        ],
    )

    do_query_test(
        "test[30s]",
        130,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[110, "1"], [120, "1"], [130, "3"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',3)]",
            ]
        ],
    )

    do_query_test(
        "test[30s]",
        131,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[110, "1"], [120, "1"], [130, "3"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',3)]",
            ]
        ],
    )

    do_query_test(
        "test[11s]",
        140,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[130, "3"], [140, "4"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:02:10.000',3),('1970-01-01 00:02:20.000',4)]",
            ]
        ],
    )

    do_query_test(
        "test[10s]",
        140,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[140, "4"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:02:20.000',4)]",
            ]
        ],
    )

    do_query_test(
        "test[9s]",
        140,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[140, "4"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:02:20.000',4)]",
            ]
        ],
    )


def test_instant_selectors():
    do_query_test(
        "test",
        129,
        '{"resultType": "vector", "result": [{"metric": {"__name__": "test"}, "value": [129, "1"]}]}',
        [
            [
                "[('__name__','test')]",
                "1970-01-01 00:02:09.000",
                "1",
            ]
        ],
    )

    do_query_test(
        "test",
        130,
        '{"resultType": "vector", "result": [{"metric": {"__name__": "test"}, "value": [130, "3"]}]}',
        [
            [
                "[('__name__','test')]",
                "1970-01-01 00:02:10.000",
                "3",
            ]
        ],
    )

    do_query_test(
        "test",
        131,
        '{"resultType": "vector", "result": [{"metric": {"__name__": "test"}, "value": [131, "3"]}]}',
        [
            [
                "[('__name__','test')]",
                "1970-01-01 00:02:11.000",
                "3",
            ]
        ],
    )


def test_function_over_time():
    do_query_test(
        "last_over_time(test[45s])[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
            ]
        ],
    )

    do_query_test(
        "idelta(test[45s])[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [165, "1"], [210, "3"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',2),('1970-01-01 00:02:30.000',1),('1970-01-01 00:02:45.000',1),('1970-01-01 00:03:30.000',3)]",
            ]
        ],
    )

    do_query_test(
        "irate(test[45s])[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.2"], [150, "0.1"], [165, "0.1"], [210, "0.3"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',0.2),('1970-01-01 00:02:30.000',0.1),('1970-01-01 00:02:45.000',0.1),('1970-01-01 00:03:30.000',0.3)]",
            ]
        ],
    )

    do_query_test(
        "test[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
            ]
        ],
    )

    do_query_test(
        "delta(test[45s])[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "3"], [150, "4.5"], [165, "2.5"], [210, "3.75"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4.5),('1970-01-01 00:02:45.000',2.5),('1970-01-01 00:03:30.000',3.75)]",
            ]
        ],
    )

    do_query_test(
        "rate(test[45s])[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.06666666666666667"], [150, "0.1"], [165, "0.05555555555555555"], [210, "0.08333333333333333"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',0.06666666666666667),('1970-01-01 00:02:30.000',0.1),('1970-01-01 00:02:45.000',0.05555555555555555),('1970-01-01 00:03:30.000',0.08333333333333333)]",
            ]
        ],
    )

    do_query_test(
        "idelta(test[35s])[120s:15s]",
        210,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [210, "3"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',2),('1970-01-01 00:02:30.000',1),('1970-01-01 00:03:30.000',3)]",
            ]
        ],
    )


def test_literals():
    timestamp = 250
    do_query_test(
        "23",
        timestamp,
        '{"resultType": "scalar", "result": [250, "23"]}',
        [["1970-01-01 00:04:10.000", 23]],
    )

    do_query_test(
        "-2.43",
        timestamp,
        '{"resultType": "scalar", "result": [250, "-2.43"]}',
        [["1970-01-01 00:04:10.000", -2.43]],
    )

    do_query_test(
        "3.4e-5",
        timestamp,
        '{"resultType": "scalar", "result": [250, "0.000034"]}',
        [["1970-01-01 00:04:10.000", "0.000034"]],
    )

    do_query_test(
        "0x8f",
        timestamp,
        '{"resultType": "scalar", "result": [250, "143"]}',
        [["1970-01-01 00:04:10.000", 143]],
    )

    do_query_test(
        "-Inf",
        timestamp,
        '{"resultType": "scalar", "result": [250, "-Inf"]}',
        [["1970-01-01 00:04:10.000", "-inf"]],
    )

    do_query_test(
        "NaN",
        timestamp,
        '{"resultType": "scalar", "result": [250, "NaN"]}',
        [["1970-01-01 00:04:10.000", "nan"]],
    )

    do_query_test(
        "1_000_000",
        timestamp,
        '{"resultType": "scalar", "result": [250, "1000000"]}',
        [["1970-01-01 00:04:10.000", "1000000"]],
    )

    do_query_test(
        ".123_456_789",
        timestamp,
        '{"resultType": "scalar", "result": [250, "0.123456789"]}',
        [["1970-01-01 00:04:10.000", "0.123456789"]],
    )

    do_query_test(
        "0x_53_AB_F3_82",
        timestamp,
        '{"resultType": "scalar", "result": [250, "1403777922"]}',
        [["1970-01-01 00:04:10.000", 1403777922]],
    )

    do_query_test(
        "1h30m",
        timestamp,
        '{"resultType": "scalar", "result": [250, "5400"]}',
        [["1970-01-01 00:04:10.000", 5400]],
    )

    do_query_test(
        "12h34m56s",
        timestamp,
        '{"resultType": "scalar", "result": [250, "45296"]}',
        [["1970-01-01 00:04:10.000", 45296]],
    )

    do_query_test(
        "54s321ms",
        timestamp,
        '{"resultType": "scalar", "result": [250, "54.321"]}',
        [["1970-01-01 00:04:10.000", "54.321"]],
    )

    do_query_test(
        "1y2w3d",
        timestamp,
        '{"resultType": "scalar", "result": [250, "33004800"]}',
        [["1970-01-01 00:04:10.000", "33004800"]],
    )

    do_query_test(
        '"this is a string"',
        timestamp,
        '{"resultType": "string", "result": [250, "this is a string"]}',
        [["1970-01-01 00:04:10.000", "this is a string"]],
    )


def test_unary_operators():
    do_query_test(
        "+test",
        180,
        '{"resultType": "vector", "result": [{"metric": {"__name__": "test"}, "value": [180, "4"]}]}',
        [["[('__name__','test')]", "1970-01-01 00:03:00.000", 4]],
    )

    do_query_test(
        "+test",
        180,
        '{"resultType": "vector", "result": [{"metric": {"__name__": "test"}, "value": [180, "4"]}]}',
        [["[('__name__','test')]", "1970-01-01 00:03:00.000", 4]],
    )

    do_query_test(
        "-test",
        180,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [180, "-4"]}]}',
        [["[]", "1970-01-01 00:03:00.000", -4]],
    )

    do_query_test(
        "(-test)[120s:15s]",
        180,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "-1"], [135, "-3"], [150, "-4"], [165, "-4"], [180, "-4"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:00.000',-1),('1970-01-01 00:02:15.000',-3),('1970-01-01 00:02:30.000',-4),('1970-01-01 00:02:45.000',-4),('1970-01-01 00:03:00.000',-4)]",
            ]
        ],
    )


def test_conversion_functions():
    do_query_test(
        "vector(1)",
        180,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [180, "1"]}]}',
        [["[]", "1970-01-01 00:03:00.000", 1]],
    )

    do_query_test(
        "vector(-1)",
        180,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [180, "-1"]}]}',
        [["[]", "1970-01-01 00:03:00.000", -1]],
    )

    do_query_test(
        "-vector(1)",
        180,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [180, "-1"]}]}',
        [["[]", "1970-01-01 00:03:00.000", -1]],
    )

    do_query_test(
        "vector(time())",
        180,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [180, "180"]}]}',
        [["[]", "1970-01-01 00:03:00.000", 180]],
    )

    do_query_test(
        "vector(1)[40:10]",
        180,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[150, "1"], [160, "1"], [170, "1"], [180, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:30.000',1),('1970-01-01 00:02:40.000',1),('1970-01-01 00:02:50.000',1),('1970-01-01 00:03:00.000',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(time())[40:10]",
        180,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[150, "150"], [160, "160"], [170, "170"], [180, "180"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:30.000',150),('1970-01-01 00:02:40.000',160),('1970-01-01 00:02:50.000',170),('1970-01-01 00:03:00.000',180)]",
            ]
        ],
    )

    do_query_test(
        "scalar(vector(1))",
        180,
        '{"resultType": "scalar", "result": [180, "1"]}',
        [["1970-01-01 00:03:00.000", 1]],
    )

    do_query_test(
        "vector(scalar(vector(time())))[40:10]",
        180,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[150, "150"], [160, "160"], [170, "170"], [180, "180"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:02:30.000',150),('1970-01-01 00:02:40.000',160),('1970-01-01 00:02:50.000',170),('1970-01-01 00:03:00.000',180)]",
            ]
        ],
    )

    do_query_test(
        "vector(scalar({http_code='404'}))[80:10]",
        180,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "1"], [120, "5"], [130, "NaN"], [140, "NaN"], [150, "NaN"], [160, "NaN"], [170, "NaN"], [180, "NaN"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',5),('1970-01-01 00:02:10.000',nan),('1970-01-01 00:02:20.000',nan),('1970-01-01 00:02:30.000',nan),('1970-01-01 00:02:40.000',nan),('1970-01-01 00:02:50.000',nan),('1970-01-01 00:03:00.000',nan)]",
            ]
        ],
    )

    do_query_test(
        "vector(scalar(last_over_time({http_code='404'}[10])))[80:10]",
        180,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "1"], [120, "5"], [130, "0"], [140, "NaN"], [150, "1"], [160, "NaN"], [170, "NaN"], [180, "NaN"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',5),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',nan),('1970-01-01 00:02:30.000',1),('1970-01-01 00:02:40.000',nan),('1970-01-01 00:02:50.000',nan),('1970-01-01 00:03:00.000',nan)]",
            ]
        ],
    )


def test_date_time_functions():
    do_query_test(
        "day_of_week(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "0"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 0]],
    )

    do_query_test(
        "day_of_week(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "0"], [120, "6"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',6)]",
            ]
        ],
    )

    do_query_test(
        "day_of_month(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "8"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 8]],
    )

    do_query_test(
        "day_of_month(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "30"], [120, "6"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',30),('1970-01-01 00:02:00.000',6)]",
            ]
        ],
    )

    do_query_test(
        "days_in_month(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "28"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 28]],
    )

    do_query_test(
        "days_in_month(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "30"], [120, "31"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',30),('1970-01-01 00:02:00.000',31)]",
            ]
        ],
    )

    do_query_test(
        "day_of_year(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "39"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 39]],
    )

    do_query_test(
        "day_of_year(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "334"], [120, "340"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',334),('1970-01-01 00:02:00.000',340)]",
            ]
        ],
    )

    do_query_test(
        "minute(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "30"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 30]],
    )

    do_query_test(
        "minute(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "30"], [120, "30"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',30),('1970-01-01 00:02:00.000',30)]",
            ]
        ],
    )

    do_query_test(
        "hour(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "20"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 20]],
    )

    do_query_test(
        "hour(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "10"], [120, "15"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',10),('1970-01-01 00:02:00.000',15)]",
            ]
        ],
    )

    do_query_test(
        "month(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "2"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 2]],
    )

    do_query_test(
        "month(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "11"], [120, "12"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',11),('1970-01-01 00:02:00.000',12)]",
            ]
        ],
    )

    do_query_test(
        "year(vector(time()))",
        1770582640,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1770582640, "2026"]}]}',
        [["[]", "2026-02-08 20:30:40.000", 2026]],
    )

    do_query_test(
        "year(timestamps)[20:10]",
        120,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[110, "2025"], [120, "2025"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:50.000',2025),('1970-01-01 00:02:00.000',2025)]",
            ]
        ],
    )


def test_math_functions():
    do_query_test(
        "abs(vector(-3))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "3"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 3]],
    )

    do_query_test(
        "abs(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "2"], [200, "1"], [300, "0.5"], [400, "0"], [500, "0.5"], [600, "1"], [700, "2"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',2),('1970-01-01 00:03:20.000',1),('1970-01-01 00:05:00.000',0.5),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.5),('1970-01-01 00:10:00.000',1),('1970-01-01 00:11:40.000',2)]",
            ]
        ],
    )

    do_query_test(
        "abs(vector(scalar(deltas)))[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[100, "2"], [200, "1"], [300, "0.5"], [400, "0"], [500, "0.5"], [600, "1"], [700, "2"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:01:40.000',2),('1970-01-01 00:03:20.000',1),('1970-01-01 00:05:00.000',0.5),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.5),('1970-01-01 00:10:00.000',1),('1970-01-01 00:11:40.000',2)]",
            ]
        ],
    )

    do_query_test(
        "sgn(vector(-3))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "-1"]}]}',
        [["[]", "1970-01-01 00:08:20.000", -1]],
    )

    do_query_test(
        "sgn(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-1"], [200, "-1"], [300, "-1"], [400, "0"], [500, "1"], [600, "1"], [700, "1"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-1),('1970-01-01 00:03:20.000',-1),('1970-01-01 00:05:00.000',-1),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',1),('1970-01-01 00:10:00.000',1),('1970-01-01 00:11:40.000',1)]",
            ]
        ],
    )

    do_query_test(
        "floor(vector(5.6))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "5"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 5]],
    )

    do_query_test(
        "floor(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-2"], [200, "-1"], [300, "-1"], [400, "0"], [500, "0"], [600, "1"], [700, "2"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-2),('1970-01-01 00:03:20.000',-1),('1970-01-01 00:05:00.000',-1),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0),('1970-01-01 00:10:00.000',1),('1970-01-01 00:11:40.000',2)]",
            ]
        ],
    )

    do_query_test(
        "ceil(vector(5.6))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "6"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 6]],
    )

    do_query_test(
        "ceil(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-2"], [200, "-1"], [300, "-0"], [400, "0"], [500, "1"], [600, "1"], [700, "2"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-2),('1970-01-01 00:03:20.000',-1),('1970-01-01 00:05:00.000',-0),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',1),('1970-01-01 00:10:00.000',1),('1970-01-01 00:11:40.000',2)]",
            ]
        ],
    )

    do_query_test(
        "sqrt(vector(1.44))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "1.2"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 1.2]],
    )

    do_query_test(
        "sqrt(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "NaN"], [300, "NaN"], [400, "0"], [500, "0.7071067811865476"], [600, "1"], [700, "1.4142135623730951"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',nan),('1970-01-01 00:05:00.000',nan),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.7071067811865476),('1970-01-01 00:10:00.000',1),('1970-01-01 00:11:40.000',1.4142135623730951)]",
            ]
        ],
    )

    do_query_test(
        "exp(vector(2))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "7.38905609893065"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 7.389056098924109]],
        eps=1e-11,  # See https://github.com/ClickHouse/ClickHouse/issues/30340
    )

    do_query_test(
        "exp(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "0.1353352832366127"], [200, "0.36787944117144233"], [300, "0.6065306597126334"], [400, "1"], [500, "1.6487212707001282"], [600, "2.718281828459045"], [700, "7.38905609893065"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',0.13533528323672805),('1970-01-01 00:03:20.000',0.3678794411711252),('1970-01-01 00:05:00.000',0.6065306597123097),('1970-01-01 00:06:40.000',1),('1970-01-01 00:08:20.000',1.6487212707014907),('1970-01-01 00:10:00.000',2.7182818284606256),('1970-01-01 00:11:40.000',7.389056098924109)]",
            ]
        ],
        eps=1e-11,  # See https://github.com/ClickHouse/ClickHouse/issues/30340
    )

    do_query_test(
        "ln(vector(2))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.6931471805599453"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.6931471805599453]],
    )

    do_query_test(
        "ln(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "NaN"], [300, "NaN"], [400, "-Inf"], [500, "-0.6931471805599453"], [600, "0"], [700, "0.6931471805599453"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',nan),('1970-01-01 00:05:00.000',nan),('1970-01-01 00:06:40.000',-inf),('1970-01-01 00:08:20.000',-0.6931471805599453),('1970-01-01 00:10:00.000',0),('1970-01-01 00:11:40.000',0.6931471805599453)]",
            ]
        ],
    )

    do_query_test(
        "log2(vector(256))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "8"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 8]],
    )

    do_query_test(
        "log2(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "NaN"], [300, "NaN"], [400, "-Inf"], [500, "-1"], [600, "0"], [700, "1"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',nan),('1970-01-01 00:05:00.000',nan),('1970-01-01 00:06:40.000',-inf),('1970-01-01 00:08:20.000',-1),('1970-01-01 00:10:00.000',0),('1970-01-01 00:11:40.000',1)]",
            ]
        ],
    )

    do_query_test(
        "log10(vector(1000000))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "6"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 6]],
    )

    do_query_test(
        "log10(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "NaN"], [300, "NaN"], [400, "-Inf"], [500, "-0.3010299956639812"], [600, "0"], [700, "0.3010299956639812"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',nan),('1970-01-01 00:05:00.000',nan),('1970-01-01 00:06:40.000',-inf),('1970-01-01 00:08:20.000',-0.3010299956639812),('1970-01-01 00:10:00.000',0),('1970-01-01 00:11:40.000',0.3010299956639812)]",
            ]
        ],
    )

    do_query_test(
        "rad(vector(30))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.5235987755982988"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.5235987755982988]],
    )

    do_query_test(
        "rad(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-0.03490658503988659"], [200, "-0.017453292519943295"], [300, "-0.008726646259971648"], [400, "0"], [500, "0.008726646259971648"], [600, "0.017453292519943295"], [700, "0.03490658503988659"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-0.03490658503988659),('1970-01-01 00:03:20.000',-0.017453292519943295),('1970-01-01 00:05:00.000',-0.008726646259971648),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.008726646259971648),('1970-01-01 00:10:00.000',0.017453292519943295),('1970-01-01 00:11:40.000',0.03490658503988659)]",
            ]
        ],
    )

    do_query_test(
        "deg(vector(pi()))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "180"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 180]],
    )

    do_query_test(
        "deg(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-114.59155902616465"], [200, "-57.29577951308232"], [300, "-28.64788975654116"], [400, "0"], [500, "28.64788975654116"], [600, "57.29577951308232"], [700, "114.59155902616465"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-114.59155902616465),('1970-01-01 00:03:20.000',-57.29577951308232),('1970-01-01 00:05:00.000',-28.64788975654116),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',28.64788975654116),('1970-01-01 00:10:00.000',57.29577951308232),('1970-01-01 00:11:40.000',114.59155902616465)]",
            ]
        ],
    )

    do_query_test(
        "sin(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.8414709848078965"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.8414709848078965]],
    )

    do_query_test(
        "sin(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-0.9092974268256816"], [200, "-0.8414709848078965"], [300, "-0.479425538604203"], [400, "0"], [500, "0.479425538604203"], [600, "0.8414709848078965"], [700, "0.9092974268256816"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-0.9092974268256817),('1970-01-01 00:03:20.000',-0.8414709848078965),('1970-01-01 00:05:00.000',-0.479425538604203),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.479425538604203),('1970-01-01 00:10:00.000',0.8414709848078965),('1970-01-01 00:11:40.000',0.9092974268256817)]",
            ]
        ],
        eps=1e-15,
    )

    do_query_test(
        "cos(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.5403023058681398"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.5403023058681398]],
    )

    do_query_test(
        "cos(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-0.4161468365471424"], [200, "0.5403023058681398"], [300, "0.8775825618903728"], [400, "1"], [500, "0.8775825618903728"], [600, "0.5403023058681398"], [700, "-0.4161468365471424"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-0.4161468365471424),('1970-01-01 00:03:20.000',0.5403023058681398),('1970-01-01 00:05:00.000',0.8775825618903728),('1970-01-01 00:06:40.000',1),('1970-01-01 00:08:20.000',0.8775825618903728),('1970-01-01 00:10:00.000',0.5403023058681398),('1970-01-01 00:11:40.000',-0.4161468365471424)]",
            ]
        ],
    )

    do_query_test(
        "tan(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "1.557407724654902"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 1.5574077246549023]],
        eps=1e-15,
    )

    do_query_test(
        "tan(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "2.185039863261519"], [200, "-1.557407724654902"], [300, "-0.5463024898437905"], [400, "0"], [500, "0.5463024898437905"], [600, "1.557407724654902"], [700, "-2.185039863261519"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',2.185039863261519),('1970-01-01 00:03:20.000',-1.5574077246549023),('1970-01-01 00:05:00.000',-0.5463024898437905),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.5463024898437905),('1970-01-01 00:10:00.000',1.5574077246549023),('1970-01-01 00:11:40.000',-2.185039863261519)]",
            ]
        ],
        eps=1e-15,
    )

    do_query_test(
        "asin(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "1.5707963267948966"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 1.5707963267948966]],
    )

    do_query_test(
        "asin(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "-1.5707963267948966"], [300, "-0.5235987755982989"], [400, "0"], [500, "0.5235987755982989"], [600, "1.5707963267948966"], [700, "NaN"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',-1.5707963267948966),('1970-01-01 00:05:00.000',-0.5235987755982991),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.5235987755982991),('1970-01-01 00:10:00.000',1.5707963267948966),('1970-01-01 00:11:40.000',nan)]",
            ]
        ],
        eps=1e-15,
    )

    do_query_test(
        "acos(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0]],
    )

    do_query_test(
        "acos(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "3.141592653589793"], [300, "2.0943951023931957"], [400, "1.5707963267948966"], [500, "1.0471975511965976"], [600, "0"], [700, "NaN"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',3.141592653589793),('1970-01-01 00:05:00.000',2.0943951023931957),('1970-01-01 00:06:40.000',1.5707963267948966),('1970-01-01 00:08:20.000',1.0471975511965974),('1970-01-01 00:10:00.000',0),('1970-01-01 00:11:40.000',nan)]",
            ]
        ],
        eps=1e-15,
    )

    do_query_test(
        "atan(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.7853981633974483"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.7853981633974483]],
    )

    do_query_test(
        "atan(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-1.1071487177940904"], [200, "-0.7853981633974483"], [300, "-0.4636476090008061"], [400, "0"], [500, "0.4636476090008061"], [600, "0.7853981633974483"], [700, "1.1071487177940904"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-1.1071487177940904),('1970-01-01 00:03:20.000',-0.7853981633974483),('1970-01-01 00:05:00.000',-0.4636476090008061),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.4636476090008061),('1970-01-01 00:10:00.000',0.7853981633974483),('1970-01-01 00:11:40.000',1.1071487177940904)]",
            ]
        ],
    )

    do_query_test(
        "sinh(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "1.1752011936438014"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 1.1752011936438014]],
    )

    do_query_test(
        "sinh(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-3.626860407847019"], [200, "-1.1752011936438014"], [300, "-0.5210953054937474"], [400, "0"], [500, "0.5210953054937474"], [600, "1.1752011936438014"], [700, "3.626860407847019"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-3.626860407847019),('1970-01-01 00:03:20.000',-1.1752011936438014),('1970-01-01 00:05:00.000',-0.5210953054937474),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.5210953054937474),('1970-01-01 00:10:00.000',1.1752011936438014),('1970-01-01 00:11:40.000',3.626860407847019)]",
            ]
        ],
    )

    do_query_test(
        "cosh(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "1.5430806348152437"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 1.5430806348152437]],
    )

    do_query_test(
        "cosh(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "3.7621956910836314"], [200, "1.5430806348152437"], [300, "1.1276259652063807"], [400, "1"], [500, "1.1276259652063807"], [600, "1.5430806348152437"], [700, "3.7621956910836314"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',3.7621956910836314),('1970-01-01 00:03:20.000',1.5430806348152437),('1970-01-01 00:05:00.000',1.1276259652063807),('1970-01-01 00:06:40.000',1),('1970-01-01 00:08:20.000',1.1276259652063807),('1970-01-01 00:10:00.000',1.5430806348152437),('1970-01-01 00:11:40.000',3.7621956910836314)]",
            ]
        ],
    )

    do_query_test(
        "tanh(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.7615941559557649"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.7615946626193841]],
        eps=1e-6,  # See https://github.com/ClickHouse/ClickHouse/issues/62390
    )

    do_query_test(
        "tanh(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-0.9640275800758169"], [200, "-0.7615941559557649"], [300, "-0.46211715726000974"], [400, "0"], [500, "0.46211715726000974"], [600, "0.7615941559557649"], [700, "0.9640275800758169"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-0.964027555388663),('1970-01-01 00:03:20.000',-0.7615947917469623),('1970-01-01 00:05:00.000',-0.46211751165947257),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.46211811616870957),('1970-01-01 00:10:00.000',0.7615946626193841),('1970-01-01 00:11:40.000',0.9640275074014772)]",
            ]
        ],
        eps=1e-6,  # See https://github.com/ClickHouse/ClickHouse/issues/62390
    )

    do_query_test(
        "asinh(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0.881373587019543"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0.881373587019543]],
    )

    do_query_test(
        "asinh(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-1.4436354751788103"], [200, "-0.881373587019543"], [300, "-0.48121182505960347"], [400, "0"], [500, "0.48121182505960347"], [600, "0.881373587019543"], [700, "1.4436354751788103"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-1.4436354751788103),('1970-01-01 00:03:20.000',-0.881373587019543),('1970-01-01 00:05:00.000',-0.48121182505960347),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.48121182505960347),('1970-01-01 00:10:00.000',0.881373587019543),('1970-01-01 00:11:40.000',1.4436354751788103)]",
            ]
        ],
    )

    do_query_test(
        "acosh(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "0"]}]}',
        [["[]", "1970-01-01 00:08:20.000", 0]],
    )

    do_query_test(
        "acosh(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "NaN"], [300, "NaN"], [400, "NaN"], [500, "NaN"], [600, "0"], [700, "1.3169578969248166"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',nan),('1970-01-01 00:05:00.000',nan),('1970-01-01 00:06:40.000',nan),('1970-01-01 00:08:20.000',nan),('1970-01-01 00:10:00.000',0),('1970-01-01 00:11:40.000',1.3169578969248166)]",
            ]
        ],
    )

    do_query_test(
        "atanh(vector(1))",
        500,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [500, "+Inf"]}]}',
        [["[]", "1970-01-01 00:08:20.000", "inf"]],
    )

    do_query_test(
        "atanh(deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "NaN"], [200, "-Inf"], [300, "-0.5493061443340548"], [400, "0"], [500, "0.5493061443340548"], [600, "+Inf"], [700, "NaN"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',nan),('1970-01-01 00:03:20.000',-inf),('1970-01-01 00:05:00.000',-0.5493061443340548),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.5493061443340548),('1970-01-01 00:10:00.000',inf),('1970-01-01 00:11:40.000',nan)]",
            ]
        ],
    )


def test_range_query():
    do_range_query_test(
        "test",
        120,
        220,
        15,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "test"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
        [
            [
                "[('__name__','test')]",
                "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
            ]
        ],
    )


def test_multiple_series_in_same_resultset():
    do_query_test(
        "rate(http_errors[100])[1:1]",
        200,
        '{"resultType": "matrix", "result": [{"metric": {"http_code": "401"}, "values": [[200, "0.04"]]}, {"metric": {"http_code": "404"}, "values": [[200, "0.07"]]}]}',
        [
            [
                "[('http_code','401')]",
                "[('1970-01-01 00:03:20.000',0.04)]",
            ],
            [
                "[('http_code','404')]",
                "[('1970-01-01 00:03:20.000',0.07)]",
            ],
        ],
    )

    # FIXME: Function sort_by_label() is not implemented yet.
    # do_query_test(
    #     "sort_by_label(rate(http_errors[100]), 'http_code')",
    #     200,
    #     '{"resultType": "vector", "result": [{"metric": {"http_code": "401"}, "value": [200, "0.04"]}, {"metric": {"http_code": "404"}, "value": [200, "0.07"]}]}',
    #     [
    #         [
    #             "[('http_code','401')]",
    #             "1970-01-01 00:03:20.000",
    #             "0.04",
    #         ],
    #         [
    #             "[('http_code','404')]",
    #             "1970-01-01 00:03:20.000",
    #             "0.07",
    #         ],
    #     ]
    # )

    do_query_test_expect_error(
        "rate({http_code='404'}[100])",
        200,
        "vector cannot contain metrics with the same labelset",
        "Multiple series have the same tags {'http_code': '404'}",
    )

    # FIXME: Function count_over_time() is not implemented yet.
    # do_query_test_expect_error(
    #     "count_over_time({http_code='404'}[10])[100:10]",
    #     200,
    #     "vector cannot contain metrics with the same labelset",
    #     "Multiple series have the same tags {'http_code': '404'}",
    # )


def test_alignment_with_subquery_step():
    do_query_test(
        "vector(1)[100:20]",
        10000,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9920, "1"], [9940, "1"], [9960, "1"], [9980, "1"], [10000, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 02:45:20.000',1),('1970-01-01 02:45:40.000',1),('1970-01-01 02:46:00.000',1),('1970-01-01 02:46:20.000',1),('1970-01-01 02:46:40.000',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(1)[100:20]",
        9999.999,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9900, "1"], [9920, "1"], [9940, "1"], [9960, "1"], [9980, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 02:45:00.000',1),('1970-01-01 02:45:20.000',1),('1970-01-01 02:45:40.000',1),('1970-01-01 02:46:00.000',1),('1970-01-01 02:46:20.000',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(1)[100:20]",
        9980,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9900, "1"], [9920, "1"], [9940, "1"], [9960, "1"], [9980, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 02:45:00.000',1),('1970-01-01 02:45:20.000',1),('1970-01-01 02:45:40.000',1),('1970-01-01 02:46:00.000',1),('1970-01-01 02:46:20.000',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(1)[100:20]",
        9979.999,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9880, "1"], [9900, "1"], [9920, "1"], [9940, "1"], [9960, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 02:44:40.000',1),('1970-01-01 02:45:00.000',1),('1970-01-01 02:45:20.000',1),('1970-01-01 02:45:40.000',1),('1970-01-01 02:46:00.000',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(1)[40.001:7.5]",
        10000,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9960, "1"], [9967.5, "1"], [9975, "1"], [9982.5, "1"], [9990, "1"], [9997.5, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 02:46:00.000',1),('1970-01-01 02:46:07.500',1),('1970-01-01 02:46:15.000',1),('1970-01-01 02:46:22.500',1),('1970-01-01 02:46:30.000',1),('1970-01-01 02:46:37.500',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(1)[40:7.5]",
        10000,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9967.5, "1"], [9975, "1"], [9982.5, "1"], [9990, "1"], [9997.5, "1"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 02:46:07.500',1),('1970-01-01 02:46:15.000',1),('1970-01-01 02:46:22.500',1),('1970-01-01 02:46:30.000',1),('1970-01-01 02:46:37.500',1)]",
            ]
        ],
    )

    do_query_test(
        "vector(1)[20:20]",
        9999,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9980, "1"]]}]}',
        [["[]", "[('1970-01-01 02:46:20.000',1)]"]],
    )

    do_query_test(
        "vector(1)[19.001:20]",
        9999,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[9980, "1"]]}]}',
        [["[]", "[('1970-01-01 02:46:20.000',1)]"]],
    )

    do_query_test(
        "vector(1)[19:20]", 9999, '{"resultType": "matrix", "result": []}', ""
    )


def test_math_binary_operators():
    do_query_test(
        "5 + 7",
        100,
        '{"resultType": "scalar", "result": [100, "12"]}',
        [["1970-01-01 00:01:40.000", 12]],
    )

    do_query_test(
        "5 - 7",
        100,
        '{"resultType": "scalar", "result": [100, "-2"]}',
        [["1970-01-01 00:01:40.000", -2]],
    )

    do_query_test(
        "5 * 7",
        100,
        '{"resultType": "scalar", "result": [100, "35"]}',
        [["1970-01-01 00:01:40.000", 35]],
    )

    do_query_test(
        "25 / 3",
        100,
        '{"resultType": "scalar", "result": [100, "8.333333333333334"]}',
        [["1970-01-01 00:01:40.000", 8.333333333333334]],
    )

    do_query_test(
        "25 % 3",
        100,
        '{"resultType": "scalar", "result": [100, "1"]}',
        [["1970-01-01 00:01:40.000", 1]],
    )

    do_query_test(
        "25 ^ 0.5",
        100,
        '{"resultType": "scalar", "result": [100, "5"]}',
        [["1970-01-01 00:01:40.000", 5]],
    )

    do_query_test(
        "-1 atan2 3",
        100,
        '{"resultType": "scalar", "result": [100, "-0.3217505543966422"]}',
        [["1970-01-01 00:01:40.000", -0.3217505543966422]],
    )

    do_query_test(
        "vector(4) + 7",
        100,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [100, "11"]}]}',
        [["[]", "1970-01-01 00:01:40.000", 11]],
    )

    do_query_test(
        "vector(time()) + 7",
        100,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [100, "107"]}]}',
        [["[]", "1970-01-01 00:01:40.000", 107]],
    )

    do_query_test(
        "deltas + 1",
        700,
        '{"resultType": "vector", "result": [{"metric": {"job": "test"}, "value": [700, "3"]}]}',
        [["[('job','test')]", "1970-01-01 00:11:40.000", 3]],
    )

    do_query_test(
        "(deltas + 1)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-1"], [200, "0"], [300, "0.5"], [400, "1"], [500, "1.5"], [600, "2"], [700, "3"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-1),('1970-01-01 00:03:20.000',0),('1970-01-01 00:05:00.000',0.5),('1970-01-01 00:06:40.000',1),('1970-01-01 00:08:20.000',1.5),('1970-01-01 00:10:00.000',2),('1970-01-01 00:11:40.000',3)]",
            ]
        ],
    )

    do_query_test(
        "(2 * deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-4"], [200, "-2"], [300, "-1"], [400, "0"], [500, "1"], [600, "2"], [700, "4"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-4),('1970-01-01 00:03:20.000',-2),('1970-01-01 00:05:00.000',-1),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',1),('1970-01-01 00:10:00.000',2),('1970-01-01 00:11:40.000',4)]",
            ]
        ],
    )

    do_query_test(
        "(deltas / 2)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-1"], [200, "-0.5"], [300, "-0.25"], [400, "0"], [500, "0.25"], [600, "0.5"], [700, "1"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-1),('1970-01-01 00:03:20.000',-0.5),('1970-01-01 00:05:00.000',-0.25),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',0.25),('1970-01-01 00:10:00.000',0.5),('1970-01-01 00:11:40.000',1)]",
            ]
        ],
    )

    do_query_test(
        "deltas + deltas",
        700,
        '{"resultType": "vector", "result": [{"metric": {"job": "test"}, "value": [700, "4"]}]}',
        [["[('job','test')]", "1970-01-01 00:11:40.000", 4]],
    )

    do_query_test(
        "(deltas + deltas)[700:100]",
        700,
        '{"resultType": "matrix", "result": [{"metric": {"job": "test"}, "values": [[100, "-4"], [200, "-2"], [300, "-1"], [400, "0"], [500, "1"], [600, "2"], [700, "4"]]}]}',
        [
            [
                "[('job','test')]",
                "[('1970-01-01 00:01:40.000',-4),('1970-01-01 00:03:20.000',-2),('1970-01-01 00:05:00.000',-1),('1970-01-01 00:06:40.000',0),('1970-01-01 00:08:20.000',1),('1970-01-01 00:10:00.000',2),('1970-01-01 00:11:40.000',4)]",
            ]
        ],
    )

    # FIXME: Function sort_by_label() is not implemented yet.
    # do_query_test(
    #     "foo + bar",
    #     150,
    #     '{"resultType": "vector", "result": [{"metric": {"shape": "square", "size": "s"}, "value": [150, "740"]}, {"metric": {"shape": "circle", "size": "l"}, "value": [150, "1016"]}]}',
    #     [
    #         ["[('shape','square'),('size','s')]", "1970-01-01 00:02:30.000", 740],
    #         ["[('shape','circle'),('size','l')]", "1970-01-01 00:02:30.000", 1016],
    #     ],
    # )

    do_query_test(
        "(foo + bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test(
        "(foo + ignoring() bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test(
        "(last_over_time(foo[10]) + last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "26"], [130, "66"], [150, "1016"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "7"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            ["[('shape','square'),('size','s')]", "[('1970-01-01 00:01:50.000',7)]"],
        ],
    )

    # FIXME: Function sort_by_label() is not implemented yet.
    # do_query_test(
    #     "foo + on(shape) bar",
    #     150,
    #     '{"resultType": "vector", "result": [{"metric": {"shape": "square"}, "value": [150, "740"]}, {"metric": {"shape": "triangle"}, "value": [150, "110"]}, {"metric": {"shape": "circle"}, "value": [150, "1016"]}]}',
    #     ''
    #     [
    #         ["[('shape','triangle')]", "1970-01-01 00:02:30.000", 110],
    #         ["[('shape','square')]", "1970-01-01 00:02:30.000", 740],
    #         ["[('shape','circle')]", "1970-01-01 00:02:30.000", 1016]
    #     ],
    # )

    do_query_test(
        "(foo + on(shape) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"shape": "square"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "16"], [120, "88"], [130, "88"], [140, "88"], [150, "110"]]}]}',
        [
            [
                "[('shape','circle')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('shape','square')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
            [
                "[('shape','triangle')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',88),('1970-01-01 00:02:10.000',88),('1970-01-01 00:02:20.000',88),('1970-01-01 00:02:30.000',110)]",
            ],
        ],
    )

    do_query_test(
        "(last_over_time(foo[10]) + on(shape) last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "26"], [130, "66"], [150, "1016"]]}, {"metric": {"shape": "square"}, "values": [[110, "7"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "16"]]}]}',
        [
            [
                "[('shape','circle')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            ["[('shape','square')]", "[('1970-01-01 00:01:50.000',7)]"],
            ["[('shape','triangle')]", "[('1970-01-01 00:01:50.000',16)]"],
        ],
    )

    do_query_test(
        "(last_over_time(foo[10]) + ignoring(size) last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "26"], [130, "66"], [150, "1016"]]}, {"metric": {"shape": "square"}, "values": [[110, "7"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "16"]]}]}',
        [
            [
                "[('shape','circle')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            ["[('shape','square')]", "[('1970-01-01 00:01:50.000',7)]"],
            ["[('shape','triangle')]", "[('1970-01-01 00:01:50.000',16)]"],
        ],
    )

    do_query_test(
        "(foo + on(shape, size) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test_expect_error(
        "(foo + on(size) bar)[50:10]",
        150,
        "matching labels must be unique on one side",
        "Multiple series have the same tags",
    )

    do_query_test(
        "(foo + on(size) group_right bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"shape": "rectangle", "size": "l"}, "values": [[110, "25"], [120, "25"], [130, "106"], [140, "106"], [150, "106"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',25),('1970-01-01 00:02:00.000',25),('1970-01-01 00:02:10.000',106),('1970-01-01 00:02:20.000',106),('1970-01-01 00:02:30.000',106)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test(
        "(bar + on(size) group_left foo)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"shape": "rectangle", "size": "l"}, "values": [[110, "25"], [120, "25"], [130, "106"], [140, "106"], [150, "106"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',25),('1970-01-01 00:02:00.000',25),('1970-01-01 00:02:10.000',106),('1970-01-01 00:02:20.000',106),('1970-01-01 00:02:30.000',106)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test(
        "(foo + on(size) group_right(__name__) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"__name__": "foo", "shape": "rectangle", "size": "l"}, "values": [[110, "25"], [120, "25"], [130, "106"], [140, "106"], [150, "106"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('__name__','foo'),('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',25),('1970-01-01 00:02:00.000',25),('1970-01-01 00:02:10.000',106),('1970-01-01 00:02:20.000',106),('1970-01-01 00:02:30.000',106)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test(
        "(bar + on(size) group_left(__name__) foo)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[110, "26"], [120, "32"], [130, "66"], [140, "66"], [150, "1016"]]}, {"metric": {"__name__": "foo", "shape": "rectangle", "size": "l"}, "values": [[110, "25"], [120, "25"], [130, "106"], [140, "106"], [150, "106"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "7"], [120, "44"], [130, "80"], [140, "740"], [150, "740"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',26),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',66),('1970-01-01 00:02:20.000',66),('1970-01-01 00:02:30.000',1016)]",
            ],
            [
                "[('__name__','foo'),('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',25),('1970-01-01 00:02:00.000',25),('1970-01-01 00:02:10.000',106),('1970-01-01 00:02:20.000',106),('1970-01-01 00:02:30.000',106)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',7),('1970-01-01 00:02:00.000',44),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',740),('1970-01-01 00:02:30.000',740)]",
            ],
        ],
    )

    do_query_test(
        '({__name__=~"foo|bar"} + on(__name__, shape) bar)[50:10]',
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "20"], [120, "32"], [130, "100"], [140, "100"], [150, "2000"]]}, {"metric": {"shape": "rectangle"}, "values": [[110, "18"], [120, "18"], [130, "180"], [140, "180"], [150, "180"]]}, {"metric": {"shape": "square"}, "values": [[110, "6"], [120, "80"], [130, "80"], [140, "1400"], [150, "1400"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "16"], [120, "16"], [130, "16"], [140, "16"], [150, "60"]]}]}',
        [
            [
                "[('shape','circle')]",
                "[('1970-01-01 00:01:50.000',20),('1970-01-01 00:02:00.000',32),('1970-01-01 00:02:10.000',100),('1970-01-01 00:02:20.000',100),('1970-01-01 00:02:30.000',2000)]",
            ],
            [
                "[('shape','rectangle')]",
                "[('1970-01-01 00:01:50.000',18),('1970-01-01 00:02:00.000',18),('1970-01-01 00:02:10.000',180),('1970-01-01 00:02:20.000',180),('1970-01-01 00:02:30.000',180)]",
            ],
            [
                "[('shape','square')]",
                "[('1970-01-01 00:01:50.000',6),('1970-01-01 00:02:00.000',80),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',1400),('1970-01-01 00:02:30.000',1400)]",
            ],
            [
                "[('shape','triangle')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',16),('1970-01-01 00:02:10.000',16),('1970-01-01 00:02:20.000',16),('1970-01-01 00:02:30.000',60)]",
            ],
        ],
    )


def test_comparison_operators_with_bool_modifier():
    # Compare scalars.
    do_query_test("5 == bool 7", 100, '{"resultType": "scalar", "result": [100, "0"]}', [["1970-01-01 00:01:40.000", 0]])
    do_query_test("5 == bool 5", 100, '{"resultType": "scalar", "result": [100, "1"]}', [["1970-01-01 00:01:40.000", 1]])
    do_query_test("5 != bool 7", 100, '{"resultType": "scalar", "result": [100, "1"]}', [["1970-01-01 00:01:40.000", 1]])
    do_query_test("5 != bool 5", 100, '{"resultType": "scalar", "result": [100, "0"]}', [["1970-01-01 00:01:40.000", 0]])
    do_query_test("5 > bool 3",  100, '{"resultType": "scalar", "result": [100, "1"]}', [["1970-01-01 00:01:40.000", 1]])
    do_query_test("5 > bool 7",  100, '{"resultType": "scalar", "result": [100, "0"]}', [["1970-01-01 00:01:40.000", 0]])
    do_query_test("5 >= bool 5", 100, '{"resultType": "scalar", "result": [100, "1"]}', [["1970-01-01 00:01:40.000", 1]])
    do_query_test("5 >= bool 7", 100, '{"resultType": "scalar", "result": [100, "0"]}', [["1970-01-01 00:01:40.000", 0]])
    do_query_test("5 < bool 7",  100, '{"resultType": "scalar", "result": [100, "1"]}', [["1970-01-01 00:01:40.000", 1]])
    do_query_test("5 < bool 3",  100, '{"resultType": "scalar", "result": [100, "0"]}', [["1970-01-01 00:01:40.000", 0]])
    do_query_test("5 <= bool 5", 100, '{"resultType": "scalar", "result": [100, "1"]}', [["1970-01-01 00:01:40.000", 1]])
    do_query_test("5 <= bool 3", 100, '{"resultType": "scalar", "result": [100, "0"]}', [["1970-01-01 00:01:40.000", 0]])

    # Compare instant vector with scalar.
    do_query_test(
        "vector(5) == bool 5",
        100,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [100, "1"]}]}',
        [["[]", "1970-01-01 00:01:40.000", 1]],
    )

    do_query_test(
        "vector(5) == bool 7",
        100,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [100, "0"]}]}',
        [["[]", "1970-01-01 00:01:40.000", 0]],
    )

    # Compare two instant vectors.
    do_query_test(
        "(foo == bool bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "0"], [120, "1"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "0"], [120, "0"], [130, "1"], [140, "0"], [150, "0"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
        ],
    )

    do_query_test(
        "(foo != bool bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "1"], [120, "0"], [130, "1"], [140, "1"], [150, "1"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "1"], [120, "1"], [130, "0"], [140, "1"], [150, "1"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
        ],
    )

    do_query_test(
        "(foo > bool bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "1"], [120, "0"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "1"], [120, "0"], [130, "0"], [140, "0"], [150, "0"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
        ],
    )

    do_query_test(
        "(foo >= bool bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "1"], [120, "1"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "1"], [120, "0"], [130, "1"], [140, "0"], [150, "0"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
        ],
    )

    do_query_test(
        "(foo < bool bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "0"], [120, "0"], [130, "1"], [140, "1"], [150, "1"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "0"], [120, "1"], [130, "0"], [140, "1"], [150, "1"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
        ],
    )

    do_query_test(
        "(foo <= bool bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "0"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "0"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
        ],
    )

    # Use on(shape) to match foo and bar by shape only, ignoring size.
    # This lets foo{shape="triangle",size="m"} match bar{shape="triangle",size="xl"},
    # which don't match by default (different size).
    do_query_test(
        "(foo > bool on(shape) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "1"], [120, "0"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square"}, "values": [[110, "1"], [120, "0"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "0"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}]}',
        [
            [
                "[('shape','circle')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','square')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','triangle')]",
                "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]",
            ],
        ],
    )

    do_query_test(
        "(foo > bool ignoring(size) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "1"], [120, "0"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square"}, "values": [[110, "1"], [120, "0"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "0"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}]}',
        [
            ["[('shape','circle')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]"],
            ["[('shape','square')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]"],
            ["[('shape','triangle')]", "[('1970-01-01 00:01:50.000',0),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]"],
        ],
    )

    # Use on(size) group_right to allow 1:many matching: size="l" has one foo (circle) but two bars
    # (circle and rectangle), which requires group_right.
    do_query_test(
        "(foo >= bool on(size) group_right bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "1"], [120, "1"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "rectangle", "size": "l"}, "values": [[110, "1"], [120, "1"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "1"], [120, "0"], [130, "1"], [140, "0"], [150, "0"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
        ],
    )

    # group_right(__name__) with bool: bool always drops __name__ unconditionally,
    # so __name__ in group_right(__name__) is ignored.
    do_query_test(
        "(foo >= bool on(size) group_right(__name__) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle", "size": "l"}, "values": [[110, "1"], [120, "1"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "rectangle", "size": "l"}, "values": [[110, "1"], [120, "1"], [130, "0"], [140, "0"], [150, "0"]]}, {"metric": {"shape": "square", "size": "s"}, "values": [[110, "1"], [120, "0"], [130, "1"], [140, "0"], [150, "0"]]}]}',
        [
            [
                "[('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',0),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
            [
                "[('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',0)]",
            ],
        ],
    )

    # on(__name__, shape) uses __name__ as part of the join key.
    # With bool modifier __name__ is dropped from the result, so the result group is just {shape}.
    do_query_test(
        '({__name__=~"foo|bar"} == bool on(__name__, shape) bar)[50:10]',
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "1"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}, {"metric": {"shape": "rectangle"}, "values": [[110, "1"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}, {"metric": {"shape": "square"}, "values": [[110, "1"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}, {"metric": {"shape": "triangle"}, "values": [[110, "1"], [120, "1"], [130, "1"], [140, "1"], [150, "1"]]}]}',
        [
            ["[('shape','circle')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]"],
            ["[('shape','rectangle')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]"],
            ["[('shape','square')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]"],
            ["[('shape','triangle')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:20.000',1),('1970-01-01 00:02:30.000',1)]"],
        ],
    )


def test_comparison_operators():
    # Compare instant vector with scalar (filter mode): keeps left values where condition is true.
    do_query_test(
        "(foo == 4)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "4"], [120, "4"]]}]}',
        [["[('__name__','foo'),('shape','square'),('size','s')]", "[('1970-01-01 00:01:50.000',4),('1970-01-01 00:02:00.000',4)]"]],
    )

    do_query_test(
        "(foo < 10)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "4"], [120, "4"]]}, {"metric": {"__name__": "foo", "shape": "triangle", "size": "m"}, "values": [[110, "8"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',4),('1970-01-01 00:02:00.000',4)]",
            ],
            [
                "[('__name__','foo'),('shape','triangle'),('size','m')]",
                "[('1970-01-01 00:01:50.000',8)]",
            ],
        ],
    )

    # Compare scalar with instant vector (filter mode): keeps right values where condition is true.
    do_query_test(
        "(8 >= bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "bar", "shape": "square", "size": "s"}, "values": [[110, "3"]]}, {"metric": {"__name__": "bar", "shape": "triangle", "size": "xl"}, "values": [[110, "8"], [120, "8"], [130, "8"], [140, "8"]]}]}',
        [
            [
                "[('__name__','bar'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',3)]",
            ],
            [
                "[('__name__','bar'),('shape','triangle'),('size','xl')]",
                "[('1970-01-01 00:01:50.000',8),('1970-01-01 00:02:00.000',8),('1970-01-01 00:02:10.000',8),('1970-01-01 00:02:20.000',8)]",
            ],
        ],
    )

    # Compare two instant vectors.
    do_query_test(
        "(foo == bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[120, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[130, "40"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:02:00.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:02:10.000',40)]",
            ],
        ],
    )

    do_query_test(
        "(foo != bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[110, "16"], [130, "16"], [140, "16"], [150, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "4"], [120, "4"], [140, "40"], [150, "40"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:10.000',16),('1970-01-01 00:02:20.000',16),('1970-01-01 00:02:30.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',4),('1970-01-01 00:02:00.000',4),('1970-01-01 00:02:20.000',40),('1970-01-01 00:02:30.000',40)]",
            ],
        ],
    )

    do_query_test(
        "(foo > bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[110, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "4"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',4)]",
            ],
        ],
    )

    do_query_test(
        "(foo >= bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[110, "16"], [120, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "4"], [130, "40"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',4),('1970-01-01 00:02:10.000',40)]",
            ],
        ],
    )

    do_query_test(
        "(foo < bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[130, "16"], [140, "16"], [150, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[120, "4"], [140, "40"], [150, "40"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:02:10.000',16),('1970-01-01 00:02:20.000',16),('1970-01-01 00:02:30.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:02:00.000',4),('1970-01-01 00:02:20.000',40),('1970-01-01 00:02:30.000',40)]",
            ],
        ],
    )

    do_query_test(
        "(foo <= bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[120, "16"], [130, "16"], [140, "16"], [150, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[120, "4"], [130, "40"], [140, "40"], [150, "40"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:02:00.000',16),('1970-01-01 00:02:10.000',16),('1970-01-01 00:02:20.000',16),('1970-01-01 00:02:30.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:02:00.000',4),('1970-01-01 00:02:10.000',40),('1970-01-01 00:02:20.000',40),('1970-01-01 00:02:30.000',40)]",
            ],
        ],
    )

    # Use on(shape) to match foo and bar by shape only, ignoring size.
    # This lets foo{shape="triangle",size="m"} match bar{shape="triangle",size="xl"},
    # which don't match by default.
    do_query_test(
        "(foo > on(shape) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"shape": "circle"}, "values": [[110, "16"]]}, {"metric": {"shape": "square"}, "values": [[110, "4"]]}, {"metric": {"shape": "triangle"}, "values": [[120, "80"], [130, "80"], [140, "80"], [150, "80"]]}]}',
        [
            ["[('shape','circle')]", "[('1970-01-01 00:01:50.000',16)]"],
            ["[('shape','square')]", "[('1970-01-01 00:01:50.000',4)]"],
            ["[('shape','triangle')]", "[('1970-01-01 00:02:00.000',80),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',80),('1970-01-01 00:02:30.000',80)]"],
        ],
    )

    do_query_test(
        "(foo > ignoring(size) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle"}, "values": [[110, "16"]]}, {"metric": {"__name__": "foo", "shape": "square"}, "values": [[110, "4"]]}, {"metric": {"__name__": "foo", "shape": "triangle"}, "values": [[120, "80"], [130, "80"], [140, "80"], [150, "80"]]}]}',
        [
            ["[('__name__','foo'),('shape','circle')]", "[('1970-01-01 00:01:50.000',16)]"],
            ["[('__name__','foo'),('shape','square')]", "[('1970-01-01 00:01:50.000',4)]"],
            ["[('__name__','foo'),('shape','triangle')]", "[('1970-01-01 00:02:00.000',80),('1970-01-01 00:02:10.000',80),('1970-01-01 00:02:20.000',80),('1970-01-01 00:02:30.000',80)]"],
        ],
    )

    # Use on(size) group_right: size="l" has one foo (circle) but two bars (circle and rectangle).
    do_query_test(
        "(foo >= on(size) group_right bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "bar", "shape": "circle", "size": "l"}, "values": [[110, "16"], [120, "16"]]}, {"metric": {"__name__": "bar", "shape": "rectangle", "size": "l"}, "values": [[110, "16"], [120, "16"]]}, {"metric": {"__name__": "bar", "shape": "square", "size": "s"}, "values": [[110, "4"], [130, "40"]]}]}',
        [
            [
                "[('__name__','bar'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',16)]",
            ],
            [
                "[('__name__','bar'),('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',16)]",
            ],
            [
                "[('__name__','bar'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',4),('1970-01-01 00:02:10.000',40)]",
            ],
        ],
    )

    # group_right(__name__) copies __name__ from the one side (foo) into the result group,
    # replacing bar's __name__
    do_query_test(
        "(foo >= on(size) group_right(__name__) bar)[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[110, "16"], [120, "16"]]}, {"metric": {"__name__": "foo", "shape": "rectangle", "size": "l"}, "values": [[110, "16"], [120, "16"]]}, {"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "values": [[110, "4"], [130, "40"]]}]}',
        [
            [
                "[('__name__','foo'),('shape','circle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','rectangle'),('size','l')]",
                "[('1970-01-01 00:01:50.000',16),('1970-01-01 00:02:00.000',16)]",
            ],
            [
                "[('__name__','foo'),('shape','square'),('size','s')]",
                "[('1970-01-01 00:01:50.000',4),('1970-01-01 00:02:10.000',40)]",
            ],
        ],
    )

    # on(__name__, shape) uses __name__ as part of the join key.
    # In filter mode __name__ is preserved in the result, so the result group is {__name__, shape}.
    do_query_test(
        '({__name__=~"foo|bar"} == on(__name__, shape) bar)[50:10]',
        150,
        '{"resultType": "matrix", "result": [{"metric": {"__name__": "bar", "shape": "circle"}, "values": [[110, "10"], [120, "16"], [130, "50"], [140, "50"], [150, "1000"]]}, {"metric": {"__name__": "bar", "shape": "rectangle"}, "values": [[110, "9"], [120, "9"], [130, "90"], [140, "90"], [150, "90"]]}, {"metric": {"__name__": "bar", "shape": "square"}, "values": [[110, "3"], [120, "40"], [130, "40"], [140, "700"], [150, "700"]]}, {"metric": {"__name__": "bar", "shape": "triangle"}, "values": [[110, "8"], [120, "8"], [130, "8"], [140, "8"], [150, "30"]]}]}',
        [
            ["[('__name__','bar'),('shape','circle')]", "[('1970-01-01 00:01:50.000',10),('1970-01-01 00:02:00.000',16),('1970-01-01 00:02:10.000',50),('1970-01-01 00:02:20.000',50),('1970-01-01 00:02:30.000',1000)]"],
            ["[('__name__','bar'),('shape','rectangle')]", "[('1970-01-01 00:01:50.000',9),('1970-01-01 00:02:00.000',9),('1970-01-01 00:02:10.000',90),('1970-01-01 00:02:20.000',90),('1970-01-01 00:02:30.000',90)]"],
            ["[('__name__','bar'),('shape','square')]", "[('1970-01-01 00:01:50.000',3),('1970-01-01 00:02:00.000',40),('1970-01-01 00:02:10.000',40),('1970-01-01 00:02:20.000',700),('1970-01-01 00:02:30.000',700)]"],
            ["[('__name__','bar'),('shape','triangle')]", "[('1970-01-01 00:01:50.000',8),('1970-01-01 00:02:00.000',8),('1970-01-01 00:02:10.000',8),('1970-01-01 00:02:20.000',8),('1970-01-01 00:02:30.000',30)]"],
        ],
    )


def test_aggregation_operators():
    do_query_test(
        "sum(bar)",
        120,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [120, "73"]}]}',
        [["[]", "1970-01-01 00:02:00.000", 73]],
    )

    do_query_test(
        "sum(last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "30"], [120, "56"], [130, "140"], [140, "700"], [150, "1030"]]}]}',
        [["[]", "[('1970-01-01 00:01:50.000',30),('1970-01-01 00:02:00.000',56),('1970-01-01 00:02:10.000',140),('1970-01-01 00:02:20.000',700),('1970-01-01 00:02:30.000',1030)]"]],
    )

    # FIXME: Not deterministic without sort_by_label(), and function sort_by_label() is not implemented yet.
    # do_query_test(
    #     "sum(bar) without (shape)",
    #     120,
    #     '{"resultType": "vector", "result": [{"metric": {"size": "l"}, "value": [120, "25"]}, {"metric": {"size": "s"}, "value": [120, "40"]}, {"metric": {"size": "xl"}, "value": [120, "8"]}]}',
    #     [
    #         ["[('size','l')]", "1970-01-01 00:02:00.000", 25],
    #         ["[('size','s')]", "1970-01-01 00:02:00.000", 40],
    #         ["[('size','xl')]", "1970-01-01 00:02:00.000", 8],
    #     ],
    # )

    do_query_test(
        "(sum(last_over_time(bar[10])) without (shape))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"size": "l"}, "values": [[110, "19"], [120, "16"], [130, "140"], [150, "1000"]]}, {"metric": {"size": "s"}, "values": [[110, "3"], [120, "40"], [140, "700"]]}, {"metric": {"size": "xl"}, "values": [[110, "8"], [150, "30"]]}]}',
        [
            ["[('size','l')]", "[('1970-01-01 00:01:50.000',19),('1970-01-01 00:02:00.000',16),('1970-01-01 00:02:10.000',140),('1970-01-01 00:02:30.000',1000)]"],
            ["[('size','s')]", "[('1970-01-01 00:01:50.000',3),('1970-01-01 00:02:00.000',40),('1970-01-01 00:02:20.000',700)]"],
            ["[('size','xl')]", "[('1970-01-01 00:01:50.000',8),('1970-01-01 00:02:30.000',30)]"],
        ],
    )

    do_query_test(
        "(count(last_over_time(bar[10])) by (size))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"size": "l"}, "values": [[110, "2"], [120, "1"], [130, "2"], [150, "1"]]}, {"metric": {"size": "s"}, "values": [[110, "1"], [120, "1"], [140, "1"]]}, {"metric": {"size": "xl"}, "values": [[110, "1"], [150, "1"]]}]}',
        [
            ["[('size','l')]", "[('1970-01-01 00:01:50.000',2),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',2),('1970-01-01 00:02:30.000',1)]"],
            ["[('size','s')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:20.000',1)]"],
            ["[('size','xl')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:30.000',1)]"],
        ],
    )

    do_query_test(
        "(avg(last_over_time(bar[10])) by (size))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"size": "l"}, "values": [[110, "9.5"], [120, "16"], [130, "70"], [150, "1000"]]}, {"metric": {"size": "s"}, "values": [[110, "3"], [120, "40"], [140, "700"]]}, {"metric": {"size": "xl"}, "values": [[110, "8"], [150, "30"]]}]}',
        [
            ["[('size','l')]", "[('1970-01-01 00:01:50.000',9.5),('1970-01-01 00:02:00.000',16),('1970-01-01 00:02:10.000',70),('1970-01-01 00:02:30.000',1000)]"],
            ["[('size','s')]", "[('1970-01-01 00:01:50.000',3),('1970-01-01 00:02:00.000',40),('1970-01-01 00:02:20.000',700)]"],
            ["[('size','xl')]", "[('1970-01-01 00:01:50.000',8),('1970-01-01 00:02:30.000',30)]"],
        ],
    )

    do_query_test(
        "min(last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "3"], [120, "16"], [130, "50"], [140, "700"], [150, "30"]]}]}',
        [["[]", "[('1970-01-01 00:01:50.000',3),('1970-01-01 00:02:00.000',16),('1970-01-01 00:02:10.000',50),('1970-01-01 00:02:20.000',700),('1970-01-01 00:02:30.000',30)]"]],
    )

    do_query_test(
        "max(last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "10"], [120, "40"], [130, "90"], [140, "700"], [150, "1000"]]}]}',
        [["[]", "[('1970-01-01 00:01:50.000',10),('1970-01-01 00:02:00.000',40),('1970-01-01 00:02:10.000',90),('1970-01-01 00:02:20.000',700),('1970-01-01 00:02:30.000',1000)]"]],
    )

    do_query_test(
        "stddev(last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "2.692582403567252"], [120, "12"], [130, "20"], [140, "0"], [150, "485"]]}]}',
        [["[]", "[('1970-01-01 00:01:50.000',2.692582403567252),('1970-01-01 00:02:00.000',12),('1970-01-01 00:02:10.000',20),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',485)]"]],
        eps=1e-9,
    )

    do_query_test(
        "stdvar(last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "7.25"], [120, "144"], [130, "400"], [140, "0"], [150, "235225"]]}]}',
        [["[]", "[('1970-01-01 00:01:50.000',7.25),('1970-01-01 00:02:00.000',144),('1970-01-01 00:02:10.000',400),('1970-01-01 00:02:20.000',0),('1970-01-01 00:02:30.000',235225)]"]],
        eps=1e-9,
    )

    # FIXME: Not deterministic without sort_by_label(), and function sort_by_label() is not implemented yet.
    # group replaces all values with 1.
    # {shape="circle", size="l"} and {shape="rectangle", size="l"} are merged to one group.
    # do_query_test(
    #     "group(bar) without (shape)",
    #     120,
    #     '{"resultType": "vector", "result": [{"metric": {"size": "l"}, "value": [120, "1"]}, {"metric": {"size": "s"}, "value": [120, "1"]}, {"metric": {"size": "xl"}, "value": [120, "1"]}]}',
    #     [
    #         ["[('size','l')]", "1970-01-01 00:02:00.000", 1],
    #         ["[('size','s')]", "1970-01-01 00:02:00.000", 1],
    #         ["[('size','xl')]", "1970-01-01 00:02:00.000", 1],
    #     ],
    # )

    do_query_test(
        "(group(last_over_time(bar[10])) without (shape))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {"size": "l"}, "values": [[110, "1"], [120, "1"], [130, "1"], [150, "1"]]}, {"metric": {"size": "s"}, "values": [[110, "1"], [120, "1"], [140, "1"]]}, {"metric": {"size": "xl"}, "values": [[110, "1"], [150, "1"]]}]}',
        [
            ["[('size','l')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',1),('1970-01-01 00:02:30.000',1)]"],
            ["[('size','s')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:20.000',1)]"],
            ["[('size','xl')]", "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:30.000',1)]"],
        ],
    )

    do_query_test(
        "quantile(0.5, last_over_time(bar[10]))[50:10]",
        150,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "8.5"], [120, "28"], [130, "70"], [140, "700"], [150, "515"]]}]}',
        [["[]", "[('1970-01-01 00:01:50.000',8.5),('1970-01-01 00:02:00.000',28),('1970-01-01 00:02:10.000',70),('1970-01-01 00:02:20.000',700),('1970-01-01 00:02:30.000',515)]"]],
    )

    # FIXME: quantile with phi depending on timestamp is not implemented yet.
    # phi = scalar(time()) / 200 varies per subquery step: 0.55, 0.60, 0.65, 0.70, 0.75.
    # do_query_test(
    #     "quantile(time() / 200, last_over_time(bar[10]))[50:10]",
    #     150,
    #     '{"resultType": "matrix", "result": [{"metric": {}, "values": [[110, "8.65"], [120, "30.4"], [130, "76"], [140, "700"], [150, "757.5"]]}]}',
    #     [["[]", "[('1970-01-01 00:01:50.000',8.65),('1970-01-01 00:02:00.000',30.4),('1970-01-01 00:02:10.000',76),('1970-01-01 00:02:20.000',700),('1970-01-01 00:02:30.000',757.5)]"]],
    # )
