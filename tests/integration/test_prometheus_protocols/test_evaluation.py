import pytest

import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
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
    sql_query = f"SELECT * FROM prometheusQuery(prometheus, {quoted_query}, {timestamp})"
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
):
    assert execute_query_in_prometheus(query, timestamp) == result
    assert execute_query_in_clickhouse_sql(query, timestamp) == TSV(chresult)
    chresult_via_http_api = execute_query_in_clickhouse_http_api(query, timestamp)
    if clickhouse_http_api_result_is_same_as_prometheus:
        assert chresult_via_http_api == result
    else:
        assert chresult_via_http_api != result


def do_query_test_expect_error(
    query,
    timestamp,
    expected_error,
    expected_cherror,
):
    assert expected_error in execute_query_in_prometheus(
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
):
    assert (
        execute_range_query_in_prometheus(query, start_time, end_time, step) == result
    )
    assert execute_range_query_in_clickhouse_sql(
        query, start_time, end_time, step
    ) == TSV(chresult)
    chresult_via_http_api = execute_range_query_in_clickhouse_http_api(
        query, start_time, end_time, step
    )
    if clickhouse_http_api_result_is_same_as_prometheus:
        assert chresult_via_http_api == result
    else:
        assert chresult_via_http_api != result


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

    # FIXME: Support unary operator '-'
    # do_query_test(
    #     "-2.43",
    #     timestamp,
    #     '{"resultType": "scalar", "result": [250, "-2.43"]}',
    #     [["1970-01-01 00:04:10.000", -2.43]],
    # )

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

    # FIXME: Support unary operator '-'
    # do_query_test(
    #     "-Inf",
    #     timestamp,
    #     '{"resultType": "scalar", "result": [250, "-Inf"]}',
    #     [["1970-01-01 00:04:10.000", "-inf"]],
    # )

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
        "\"this is a string\"",
        timestamp,
        '{"resultType": "string", "result": [250, "this is a string"]}',
        [["1970-01-01 00:04:10.000", "this is a string"]],
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
