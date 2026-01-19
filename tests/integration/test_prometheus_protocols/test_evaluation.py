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
def execute_query_in_prometheus_reader(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_reader_ip,
        cluster.prometheus_reader_port,
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in the "prometheus_receiver" service. We sent data to this service earlier via the RemoteWrite protocol.
def execute_query_in_prometheus_receiver(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_receiver_ip,
        cluster.prometheus_receiver_port,
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in both prometheus services - results should be the same.
def execute_query_in_prometheus(query, timestamp):
    r1 = execute_query_in_prometheus_reader(query, timestamp)
    r2 = execute_query_in_prometheus_receiver(query, timestamp)
    assert r1 == r2
    return r1

# Executes a prometheus query in ClickHouse via HTTP API
def execute_query_in_clickhouse_http_api(query, timestamp):
    return execute_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query",
        query,
        timestamp,
    )

# Executes a prometheus query in ClickHouse via SQL query
def execute_query_in_clickhouse_sql(query, timestamp):
    return node.query(
        f"SELECT * FROM prometheusQuery(prometheus, '{query}', {timestamp})"
    )


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


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        yield cluster
    finally:
        cluster.shutdown()


def test_up():
    send_data([({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})])

    assert (
        execute_query_in_prometheus("up", 1753176757.89)
        == '{"resultType": "vector", "result": [{"metric": {"__name__": "up", "job": "prometheus"}, "value": [1753176757.89, "1"]}]}'
    )

    assert execute_query_in_clickhouse_sql("up", 1753176757.89) == TSV(
        [["[('__name__','up'),('job','prometheus')]", "2025-07-22 09:32:37.890", "1"]]
    )


def send_test_data():
    send_data(
        [
            (
                {"__name__": "test_data"},
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


def test_first():
    send_test_data()

    queries = [  # array of tuples (query, timestamp, http_api_result, result of prometheusQuery, flag is ClickHouse HTTP API result is same as Prometheus one)
        (
            "test_data",
            129,
            '{"resultType": "vector", "result": [{"metric": {"__name__": "test_data"}, "value": [129, "1"]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "1970-01-01 00:02:09.000",
                    "1",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data",
            130,
            '{"resultType": "vector", "result": [{"metric": {"__name__": "test_data"}, "value": [130, "3"]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "1970-01-01 00:02:10.000",
                    "3",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data",
            131,
            '{"resultType": "vector", "result": [{"metric": {"__name__": "test_data"}, "value": [131, "3"]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "1970-01-01 00:02:11.000",
                    "3",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data[30s]",
            129,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[110, "1"], [120, "1"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data[30s]",
            130,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[110, "1"], [120, "1"], [130, "3"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',3)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data[30s]",
            131,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[110, "1"], [120, "1"], [130, "3"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',3)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data[11s]",
            140,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[130, "3"], [140, "4"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:10.000',3),('1970-01-01 00:02:20.000',4)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data[10s]",
            140,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[140, "4"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:20.000',4)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "test_data[9s]",
            140,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[140, "4"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:20.000',4)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "last_over_time(test_data[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "idelta(test_data[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [165, "1"], [210, "3"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',2),('1970-01-01 00:02:30.000',1),('1970-01-01 00:02:45.000',1),('1970-01-01 00:03:30.000',3)]",
                ]
            ],
            # FIXME: Results are different!
            # | E   AssertionError: query: idelta(test_data[45s])[120s:15s]
            # | E   assert '{"resultType...210, "3"]]}]}' == '{"resultType...210, "3"]]}]}'
            # | E
            # | E     - {"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [165, "1"], [210, "3"]]}]}
            # | E     + {"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "0"], [135, "2"], [150, "1"], [165, "1"], [210, "3"]]}]}
            False
        ),
        (
            "irate(test_data[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.2"], [150, "0.1"], [165, "0.1"], [210, "0.3"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',0.2),('1970-01-01 00:02:30.000',0.1),('1970-01-01 00:02:45.000',0.1),('1970-01-01 00:03:30.000',0.3)]",
                ]
            ],
            # FIXME: Results are different!
            # | E   AssertionError: query: irate(test_data[45s])[120s:15s]
            # | E   assert '{"resultType...0, "0.3"]]}]}' == '{"resultType...0, "0.3"]]}]}'
            # | E
            # | E     - {"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.2"], [150, "0.1"], [165, "0.1"], [210, "0.3"]]}]}
            # | E     + {"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "0"], [135, "0.2"], [150, "0.1"], [165, "0.1"], [210, "0.3"]]}]}
            False
        ),
        (
            "test_data[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
                ]
            ],
            # Everything is Ok
            True
        ),
        (
            "delta(test_data[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "3"], [150, "4.5"], [165, "2.5"], [210, "3.75"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4.5),('1970-01-01 00:02:45.000',2.5),('1970-01-01 00:03:30.000',3.75)]",
                ]
            ],
            # FIXME: Results are different!
            # | E   AssertionError: query: delta(test_data[45s])[120s:15s]
            # | E   assert '{"resultType..., "3.75"]]}]}' == '{"resultType..., "3.75"]]}]}'
            # | E
            # | E     - {"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "3"], [150, "4.5"], [165, "2.5"], [210, "3.75"]]}]}
            # | E     + {"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "0"], [135, "3"], [150, "4.5"], [165, "2.5"], [210, "3.75"]]}]}
            # | E     ?
            False
        ),
        (
            "rate(test_data[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.06666666666666667"], [150, "0.1"], [165, "0.05555555555555555"], [210, "0.08333333333333333"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',0.06666666666666667),('1970-01-01 00:02:30.000',0.1),('1970-01-01 00:02:45.000',0.05555555555555555),('1970-01-01 00:03:30.000',0.08333333333333333)]",
                ]
            ],
            # FIXME: Results are different!
            # | E   AssertionError: query: rate(test_data[45s])[120s:15s]
            # | E   assert '{"resultType..., "0.08"]]}]}' == '{"resultType...3333333"]]}]}'
            # | E
            # | E     - {"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.06666666666666667"], [150, "0.1"], [165, "0.05555555555555555"], [210, "0.08333333333333333"]]}]}
            # | E     ?                                                                                     ---------------                             ^^^^^^^^^^^^^^^^               ---------------
            # | E     + {"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "0"], [135, "0.07"], [150, "0.1"], [165, "0.06"], [210, "0.08"]]}]}
            # | E     ?                                                 ++++++++...
            # | E
            False
        ),
        (
            "idelta(test_data[35s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [210, "3"]]}]}',
            [
                [
                    "[('__name__','test_data')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',2),('1970-01-01 00:02:30.000',1),('1970-01-01 00:03:30.000',3)]",
                ]
            ],
            # FIXME: Results are different!
            # | E   AssertionError: query: idelta(test_data[35s])[120s:15s]
            # | E   assert '{"resultType...210, "3"]]}]}' == '{"resultType...210, "3"]]}]}'
            # | E
            # | E     - {"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [210, "3"]]}]}
            # | E     + {"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "0"], [135, "2"], [150, "1"], [210, "3"]]}]}
            # | E     ?
            False
        ),
    ]

    for query, timestamp, result, chresult, clickhouse_http_api_result_is_same_as_prometheus in queries:
        assert (
            execute_query_in_prometheus(query, timestamp) == result
        ), f"query: {query}"
        assert execute_query_in_clickhouse_sql(query, timestamp) == TSV(
            chresult
        ), f"query: {query}"
        if not clickhouse_http_api_result_is_same_as_prometheus:
            continue
        assert (
            execute_query_in_clickhouse_http_api(query, timestamp) == result
        ), f"query: {query}"


def test_range_query():
    send_test_data()

    query = "test_data"
    start_time = 120
    end_time = 220
    step = 15

    result = '{"resultType": "matrix", "result": [{"metric": {"__name__": "test_data"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}'
    chresult = [
        [
            "[('__name__','test_data')]",
            "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
        ]
    ]

    assert (
        execute_range_query_in_prometheus(query, start_time, end_time, step)
        == result
    )

    assert (
        execute_range_query_in_clickhouse_sql(query, start_time, end_time, step)
        == TSV(chresult)
    )

    assert (
        execute_range_query_in_clickhouse_http_api(query, start_time, end_time, step)
        == result
    )
