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
    return execute_instant_query_with_http_api(
        cluster.prometheus_reader_ip,
        cluster.prometheus_reader_port,
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in the "prometheus_receiver" service. We sent data to this service earlier via the RemoteWrite protocol.
def execute_query_in_prometheus_receiver(query, timestamp):
    return execute_instant_query_with_http_api(
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


def test_up():
    send_data([({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})])

    assert (
        execute_query_in_prometheus("up", 1753176757.89)
        == '{"resultType": "vector", "result": [{"metric": {"__name__": "up", "job": "prometheus"}, "value": [1753176757.89, "1"]}]}'
    )

    assert execute_query_in_clickhouse("up", 1753176757.89) == TSV(
        [["[('__name__','up'),('job','prometheus')]", "2025-07-22 09:32:37.890", "1"]]
    )


def test_grid():
    send_data(
        [
            (
                {"__name__": "grid"},
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

    queries = [  # array of tuples (query, timestamp, http_api_result, result of prometheusQuery)
        (
            "grid",
            129,
            '{"resultType": "vector", "result": [{"metric": {"__name__": "grid"}, "value": [129, "1"]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "1970-01-01 00:02:09.000",
                    "1",
                ]
            ],
        ),
        (
            "grid",
            130,
            '{"resultType": "vector", "result": [{"metric": {"__name__": "grid"}, "value": [130, "3"]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "1970-01-01 00:02:10.000",
                    "3",
                ]
            ],
        ),
        (
            "grid",
            131,
            '{"resultType": "vector", "result": [{"metric": {"__name__": "grid"}, "value": [131, "3"]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "1970-01-01 00:02:11.000",
                    "3",
                ]
            ],
        ),
        (
            "grid[30s]",
            129,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[110, "1"], [120, "1"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1)]",
                ]
            ],
        ),
        (
            "grid[30s]",
            130,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[110, "1"], [120, "1"], [130, "3"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',3)]",
                ]
            ],
        ),
        (
            "grid[30s]",
            131,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[110, "1"], [120, "1"], [130, "3"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:01:50.000',1),('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:10.000',3)]",
                ]
            ],
        ),
        (
            "grid[11s]",
            140,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[130, "3"], [140, "4"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:10.000',3),('1970-01-01 00:02:20.000',4)]",
                ]
            ],
        ),
        (
            "grid[10s]",
            140,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[140, "4"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:20.000',4)]",
                ]
            ],
        ),
        (
            "grid[9s]",
            140,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[140, "4"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:20.000',4)]",
                ]
            ],
        ),
        (
            "grid[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
                ]
            ],
        ),
        (
            "last_over_time(grid[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {"__name__": "grid"}, "values": [[120, "1"], [135, "3"], [150, "4"], [165, "4"], [180, "4"], [195, "5"], [210, "8"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:00.000',1),('1970-01-01 00:02:15.000',3),('1970-01-01 00:02:30.000',4),('1970-01-01 00:02:45.000',4),('1970-01-01 00:03:00.000',4),('1970-01-01 00:03:15.000',5),('1970-01-01 00:03:30.000',8)]",
                ]
            ],
        ),
        (
            "irate(grid[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.2"], [150, "0.1"], [165, "0.1"], [210, "0.3"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',0.2),('1970-01-01 00:02:30.000',0.1),('1970-01-01 00:02:45.000',0.1),('1970-01-01 00:03:30.000',0.3)]",
                ]
            ],
        ),
        (
            "idelta(grid[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "2"], [150, "1"], [165, "1"], [210, "3"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',2),('1970-01-01 00:02:30.000',1),('1970-01-01 00:02:45.000',1),('1970-01-01 00:03:30.000',3)]",
                ]
            ],
        ),
        (
            "rate(grid[45s])[120s:15s]",
            210,
            '{"resultType": "matrix", "result": [{"metric": {}, "values": [[120, "0"], [135, "0.06666666666666667"], [150, "0.1"], [165, "0.05555555555555555"], [210, "0.08333333333333333"]]}]}',
            [
                [
                    "[('__name__','grid')]",
                    "[('1970-01-01 00:02:00.000',0),('1970-01-01 00:02:15.000',0.0666681481810707),('1970-01-01 00:02:30.000',0.1),('1970-01-01 00:02:45.000',0.0555545678792862),('1970-01-01 00:03:30.000',0.08333518522633837)]", #TODO
                ]
            ],
        ),
    ]

    for query, timestamp, result, chresult in queries:
        assert execute_query_in_prometheus(query, timestamp) == result
        assert execute_query_in_clickhouse(query, timestamp) == TSV(chresult)


def test_temp():

    send_data(
        [
            (
                {"__name__": "grid"},
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

    res = execute_range_query_with_http_api(
        cluster.prometheus_receiver_ip,
        cluster.prometheus_receiver_port,
        "/api/v1/query_range",
        "grid",
        122, 203, 10,
    )
    print(f"range_result={res}")
    assert False
