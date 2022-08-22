#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -mn -q """
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (x UInt64, y UInt64, val UInt64) ENGINE = MergeTree ORDER BY (x, y)
AS SELECT sipHash64(number, '11') % 1000 AS x, sipHash64(number, '12') % 1000 AS y, sipHash64(number, '13') % 1000 FROM numbers(1000);

CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b)
AS SELECT sipHash64(number, '21') % 1000 AS a, sipHash64(number, '22') % 1000 AS b FROM numbers(1000);

CREATE TABLE t3 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY (x, y)
AS SELECT sipHash64(number, '21') % 1000 AS x, sipHash64(number, '22') % 1000 AS y FROM numbers(1000);
"""

# test_query <expected_number_of_sort_steps> <query>
function test_query {
    expected_result=${1}
    query_str="${2}"

    # count the number of PartialSortingTransform's in the pipeline
    result=$( $CLICKHOUSE_CLIENT \
        --join_algorithm=full_sorting_merge \
        --optimize_sorting_by_input_stream_properties=1 \
        --optimize_read_in_order=1 --max_threads=8 --max_block_size=128 \
        -q "EXPLAIN PIPELINE ${query_str}" | grep 'PartialSortingTransform' | wc -l )

    test "$result" -eq "$expected_result" || echo "fail ${BASH_LINENO[0]}: $expected_result != $result in '${query_str}'"
}

test_query 0 'SELECT * FROM t1 JOIN t3 USING (x)'
test_query 0 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x'

test_query 0 'SELECT * FROM t1 JOIN t3 USING (x, y)'
test_query 0 'SELECT * FROM t1 JOIN t3 USING (y, x)'
test_query 0 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x AND t1.y = t3.y'
test_query 0 'SELECT * FROM t1 JOIN t3 ON t1.y = t3.y AND t1.x = t3.x'

test_query 0 'SELECT * FROM t1 JOIN t2 ON t1.x = t2.a'
test_query 0 'SELECT * FROM t1 JOIN t2 ON x = a'

test_query 0 'SELECT * FROM t1 JOIN t2 ON t1.x = t2.a AND t1.y = t2.b AND t1.x = t2.b'

test_query 0 'SELECT * FROM t1 JOIN t2 ON t1.x = t2.a AND t1.y = t2.b'
test_query 0 'SELECT * FROM t1 JOIN t2 ON t1.y = t2.b AND t1.x = t2.a'
test_query 0 'SELECT * FROM t1 JOIN t2 ON x = a AND y = b'
test_query 0 'SELECT * FROM t1 JOIN t2 ON y = b AND x = a'

test_query 0 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x AND t1.x = t3.y'

# sort only one stream
test_query 1 'SELECT * FROM t1 JOIN t3 ON t1.val = t3.x AND t1.y = t3.y'
test_query 1 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.y AND t1.y = t3.y'
test_query 1 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.y AND t1.x = t3.y'

# two sorts for inner subqueries, don't resort
test_query 2 '
    SELECT * FROM ( SELECT x, y FROM (SELECT number AS x, number AS y FROM numbers_mt(1000))
        ORDER BY x, y DESC ) AS t1
    JOIN ( SELECT a, b FROM (SELECT number AS a, number AS b FROM numbers_mt(1000))
        ORDER BY a, b DESC ) AS t2
    ON t1.x = t2.a AND t1.y = t2.b'

# two sorts for inner subqueries + resort one stream, because it's sorted in different direction
test_query 3 '
    SELECT * FROM ( SELECT x, y FROM (SELECT number AS x, number AS y FROM numbers_mt(1000))
        ORDER BY x DESC, y DESC ) AS t1
    JOIN ( SELECT a, b FROM (SELECT number AS a, number AS b FROM numbers_mt(1000))
        ORDER BY a, b DESC ) AS t2
    ON t1.x = t2.a AND t1.y = t2.b'

test_query 3 '
    SELECT * FROM ( SELECT x, y FROM (SELECT number AS x, number AS y FROM numbers_mt(1000))
        ORDER BY x, y DESC ) AS t1
    JOIN ( SELECT a, b FROM (SELECT number AS a, number AS b FROM numbers_mt(1000))
        ORDER BY a DESC, b DESC ) AS t2
    ON t1.x = t2.a AND t1.y = t2.b'
