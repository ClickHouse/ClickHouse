import json

from .test_evaluation import (
    do_query_test,
    do_range_query_test,
    execute_query_in_clickhouse_http_api,
    start_cluster as start_cluster,
)


def test_standalone_start_end_instant():
    # Prometheus 3.7.3 does not support these functions, so compare with constant equivalents.
    do_query_test(
        "start()",
        1000.125,
        '{"resultType": "scalar", "result": [1000.125, "1000.125"]}',
        [["1970-01-01 00:16:40.125", 1000.125]],
        prometheus_query="1000.125",
    )
    do_query_test(
        "end() - start()",
        1750000000.125,
        '{"resultType": "scalar", "result": [1750000000.125, "0"]}',
        [["2025-06-15 15:06:40.125", 0]],
        prometheus_query="0",
    )

    # Nested subqueries and offsets must not change the outer query's start.
    do_query_test(
        "last_over_time(last_over_time(vector(start())[10s:2s])[20s:5s] offset 7s)",
        1000.125,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1000.125, "1000.125"]}]}',
        [["[]", "1970-01-01 00:16:40.125", 1000.125]],
        prometheus_query="last_over_time(last_over_time(vector(1000.125)[10s:2s])[20s:5s] offset 7s)",
    )

    # An explicit subquery timestamp must not replace the outer query's end.
    do_query_test(
        "last_over_time(vector(end())[20s:5s] @ 1005)",
        1750000000.125,
        '{"resultType": "vector", "result": [{"metric": {}, "value": [1750000000.125, "1750000000.125"]}]}',
        [["[]", "2025-06-15 15:06:40.125", 1750000000.125]],
        prometheus_query="last_over_time(vector(1750000000.125)[20s:5s] @ 1005)",
    )

    # Omit the HTTP timestamp to exercise the default current evaluation time.
    data = json.loads(
        execute_query_in_clickhouse_http_api("start() - time() + end() - time()", None)
    )
    assert data["resultType"] == "scalar"
    assert float(data["result"][1]) == 0


def test_standalone_start_end_range():
    # The requested end falls between evaluation steps and must not be rounded down.
    do_range_query_test(
        "vector(end() - start())",
        1000.125,
        1002.375,
        0.5,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[1000.125, "2.25"], [1000.625, "2.25"], [1001.125, "2.25"], [1001.625, "2.25"], [1002.125, "2.25"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:16:40.125',2.25),('1970-01-01 00:16:40.625',2.25),('1970-01-01 00:16:41.125',2.25),('1970-01-01 00:16:41.625',2.25),('1970-01-01 00:16:42.125',2.25)]",
            ]
        ],
        prometheus_query="vector(2.25)",
    )

    # One-point range queries return the same timestamp for both boundaries.
    do_range_query_test(
        "vector(start() + end())",
        1000.125,
        1000.125,
        7.5,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[1000.125, "2000.25"]]}]}',
        [["[]", "[('1970-01-01 00:16:40.125',2000.25)]"]],
        prometheus_query="vector(2000.25)",
    )

    # Subqueries and offsets must not replace the outer start.
    do_range_query_test(
        "last_over_time(vector(start())[20s:5s] offset 7s)",
        1000.125,
        1002.375,
        0.5,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[1000.125, "1000.125"], [1000.625, "1000.125"], [1001.125, "1000.125"], [1001.625, "1000.125"], [1002.125, "1000.125"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:16:40.125',1000.125),('1970-01-01 00:16:40.625',1000.125),('1970-01-01 00:16:41.125',1000.125),('1970-01-01 00:16:41.625',1000.125),('1970-01-01 00:16:42.125',1000.125)]",
            ]
        ],
        prometheus_query="last_over_time(vector(1000.125)[20s:5s] offset 7s)",
    )

    # An explicit subquery timestamp must not replace the requested off-grid end.
    do_range_query_test(
        "last_over_time(vector(end())[20s:5s] @ 1005)",
        1000.125,
        1002.375,
        0.5,
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[1000.125, "1002.375"], [1000.625, "1002.375"], [1001.125, "1002.375"], [1001.625, "1002.375"], [1002.125, "1002.375"]]}]}',
        [
            [
                "[]",
                "[('1970-01-01 00:16:40.125',1002.375),('1970-01-01 00:16:40.625',1002.375),('1970-01-01 00:16:41.125',1002.375),('1970-01-01 00:16:41.625',1002.375),('1970-01-01 00:16:42.125',1002.375)]",
            ]
        ],
        prometheus_query="last_over_time(vector(1002.375)[20s:5s] @ 1005)",
    )

    # Keep millisecond precision at modern timestamps without narrowing to Float32.
    do_range_query_test(
        "vector(end())",
        "1750000000.001",
        "1750000000.003",
        "0.001",
        '{"resultType": "matrix", "result": [{"metric": {}, "values": [[1750000000.001, "1750000000.003"], [1750000000.002, "1750000000.003"], [1750000000.003, "1750000000.003"]]}]}',
        [
            [
                "[]",
                "[('2025-06-15 15:06:40.001',1750000000.003),('2025-06-15 15:06:40.002',1750000000.003),('2025-06-15 15:06:40.003',1750000000.003)]",
            ]
        ],
        prometheus_query="vector(1750000000.003)",
    )
