#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function explain_sort_description()
{
    out=$($CLICKHOUSE_CLIENT --optimize_read_in_order=1 -q "EXPLAIN PLAN actions = 1 $1")
    echo "$out" | grep "Prefix sort description:"
    echo "$out" | grep "Result sort description:"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_order_by_monotonic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_order_by_monotonic (t DateTime, c1 String) ENGINE = MergeTree ORDER BY (t, c1)
    AS SELECT '2022-09-09 12:00:00', toString(number % 2) FROM numbers(2) UNION ALL
       SELECT '2022-09-09 12:00:30', toString(number % 2)|| 'x' FROM numbers(3)"

$CLICKHOUSE_CLIENT --optimize_aggregation_in_order=1 -q "SELECT count() FROM
    (SELECT toStartOfMinute(t) AS s, c1 FROM t_order_by_monotonic GROUP BY s, c1)"

$CLICKHOUSE_CLIENT --optimize_read_in_order=1 -q "SELECT toStartOfMinute(t) AS s, c1 FROM t_order_by_monotonic ORDER BY s, c1"

explain_sort_description "SELECT toStartOfMinute(t) AS s, c1 FROM t_order_by_monotonic ORDER BY s, c1"
explain_sort_description "SELECT toStartOfMinute(t) AS s, c1 FROM t_order_by_monotonic ORDER BY s"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_order_by_monotonic"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_order_by_monotonic (a Int64, b Int64) ENGINE = MergeTree ORDER BY (a, b)"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_order_by_monotonic VALUES (1, 1) (1, 2), (2, 1) (2, 2)"

explain_sort_description "SELECT * FROM t_order_by_monotonic ORDER BY -a"
explain_sort_description "SELECT * FROM t_order_by_monotonic ORDER BY -a, -b"
explain_sort_description "SELECT * FROM t_order_by_monotonic ORDER BY a DESC, -b"
explain_sort_description "SELECT * FROM t_order_by_monotonic ORDER BY -a, b DESC"
explain_sort_description "SELECT * FROM t_order_by_monotonic ORDER BY -a, b"
explain_sort_description "SELECT * FROM t_order_by_monotonic ORDER BY a, -b"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_order_by_monotonic"
