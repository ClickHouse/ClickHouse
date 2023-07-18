#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -mn -q "
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS ta1;
DROP TABLE IF EXISTS ta2;

CREATE TABLE t1 (x UInt64, y UInt64, val UInt64) ENGINE = MergeTree ORDER BY (x, y) PARTITION BY (x % 2)
AS SELECT sipHash64(number, '11') % 100, sipHash64(number, '12') % 100, sipHash64(number, '13') % 100 FROM numbers(1000);

CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a % 2)
AS SELECT sipHash64(number, '21') % 100, sipHash64(number, '22') % 100 FROM numbers(1000);

CREATE TABLE t3 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY (x, y) PARTITION BY (x % 2)
AS SELECT sipHash64(number, '21') % 100, sipHash64(number, '22') % 100 FROM numbers(1000);

CREATE TABLE t4 (y UInt64, x UInt64) ENGINE = MergeTree ORDER BY (y, x) PARTITION BY (y % 2)
AS SELECT sipHash64(number, '21') % 100, sipHash64(number, '22') % 100 FROM numbers(1000);

CREATE TABLE ta1 (a1 UInt64, a2 UInt64, a3 UInt64, a4 UInt64, a5 UInt64, a6 UInt64, a7 UInt64, a8 UInt64, a9 UInt64, a10 UInt64)
ENGINE = MergeTree ORDER BY (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) PARTITION BY (a1 % 2)
AS SELECT
    sipHash64(number, 'a11') % 2, sipHash64(number, 'a12') % 2, sipHash64(number, 'a13') % 2, sipHash64(number, 'a14') % 2, sipHash64(number, 'a15') % 2,
    sipHash64(number, 'a16') % 2, sipHash64(number, 'a17') % 2, sipHash64(number, 'a18') % 2, sipHash64(number, 'a19') % 2, sipHash64(number, 'a20') % 2
FROM numbers(1000);

CREATE TABLE ta2 (a1 UInt64, a2 UInt64, a3 UInt64, a4 UInt64, a5 UInt64, a6 UInt64, a7 UInt64, a8 UInt64, a9 UInt64, a10 UInt64)
ENGINE = MergeTree ORDER BY (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) PARTITION BY (a1 % 2)
AS SELECT
    sipHash64(number, 'a21') % 2, sipHash64(number, 'a22') % 2, sipHash64(number, 'a23') % 2, sipHash64(number, 'a24') % 2, sipHash64(number, 'a25') % 2,
    sipHash64(number, 'a26') % 2, sipHash64(number, 'a27') % 2, sipHash64(number, 'a28') % 2, sipHash64(number, 'a29') % 2, sipHash64(number, 'a30') % 2
FROM numbers(1000);
"

CLIENT_ARGS='--join_algorithm=full_sorting_merge --optimize_sorting_by_input_stream_properties=1 --optimize_read_in_order=1 --max_threads=8 --max_block_size=128'

# test_query <expected_number_of_tables_to_read_in_order> <query>
function test_query {
    expected_result=${1}
    query_str="${2}"

    # count the number of MergeSortingTransform's in the pipeline
    result=$( $CLICKHOUSE_CLIENT $CLIENT_ARGS -q "EXPLAIN PIPELINE ${query_str}" )
    read_in_order_count=$( echo "$result" | grep 'MergeTreeInOrder' | wc -l )

    test "$read_in_order_count" -eq "$expected_result" || echo "fail ${BASH_LINENO[0]}: expected: $expected_result, got: $read_in_order_count in '${query_str}'"

    echo "$result" | grep -q 'FilterOnTheFly' && echo "fail ${BASH_LINENO[0]}: step FilterOnTheFly should not be present for read in order"
}

# Simple case: join by sorted prefix with length 1
test_query 2 'SELECT * FROM t1 JOIN t3 USING (x)'
test_query 2 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x'
test_query 2 'SELECT * FROM t1 JOIN t4 ON t1.x = t4.y'

# Join by sorted prefix with length 2
test_query 2 'SELECT * FROM t1 JOIN t3 USING (x, y)'
# Keys should be deduplicated
test_query 2 'SELECT * FROM t1 JOIN t3 USING (x, x, x, y, y, y, y, x, x, x)'
# Keys can be reordered
test_query 2 'SELECT * FROM t1 JOIN t3 USING (y, x)'

# Reoderinbg and deduplication for ON syntax:
test_query 2 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x AND t1.y = t3.y'
test_query 2 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x AND t1.y = t3.y AND t1.y = t3.y AND t1.y = t3.y AND t1.x = t3.x AND t1.x = t3.x'
test_query 2 'SELECT * FROM t1 JOIN t3 ON t1.y = t3.y AND t1.x = t3.x'
test_query 2 'SELECT * FROM t1 JOIN t4 ON t1.x = t4.y AND t1.y = t4.x'
test_query 2 'SELECT * FROM t1 JOIN t4 ON t1.y = t4.x AND t1.x = t4.y'

# Names doesn't matter (`x,y` or `a,b` in the table)
test_query 2 'SELECT * FROM t1 JOIN t2 ON t1.x = t2.a'
test_query 2 'SELECT * FROM t1 JOIN t2 ON x = a'

test_query 2 'SELECT * FROM t1 JOIN t2 ON t1.x = t2.a AND t1.y = t2.b'
test_query 2 'SELECT * FROM t1 JOIN t2 ON t1.y = t2.b AND t1.x = t2.a'
test_query 2 'SELECT * FROM t1 JOIN t2 ON x = a AND y = b'
test_query 2 'SELECT * FROM t1 JOIN t2 ON y = b AND x = a'

test_query 2 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.x AND t1.x = t3.y'

# USING with any order of 10 keys
test_query 2 'SELECT * FROM ta1 JOIN ta2 USING (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)'
test_query 2 'SELECT * FROM ta1 JOIN ta2 USING (a10, a9, a8, a7, a6, a5, a4, a3, a2, a1)'
test_query 2 'SELECT * FROM ta1 JOIN ta2 USING (a9, a7, a8, a10, a5, a3, a6, a4, a1, a2)'

# JOIN ON with 10 keys
test_query 2 'SELECT * FROM ta1 JOIN t1 ON ta1.a1 = t1.x AND ta1.a2 = t1.x AND ta1.a3 = t1.x AND ta1.a4 = t1.x AND ta1.a5 = t1.x
                                       AND ta1.a6 = t1.x AND ta1.a7 = t1.y AND ta1.a8 = t1.y AND ta1.a9 = t1.y AND ta1.a10 = t1.y'

# sort only one stream that is joined not by prefix
test_query 1 'SELECT * FROM t1 JOIN t3 ON t1.val = t3.x AND t1.y = t3.y'
test_query 1 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.y AND t1.y = t3.y'
test_query 1 'SELECT * FROM t1 JOIN t3 ON t1.x = t3.y AND t1.x = t3.y'

# ta1 should be sorted, since (a2, a3) is not a sorting prefix
test_query 1 'SELECT * FROM ta1 JOIN ta2 ON ta1.a2 = ta2.a1 AND ta1.a3 = ta2.a2 AND ta1.a2 = ta2.a3 AND ta1.a2 = ta2.a4'

# sort for inner subqueries and resort since prefixes are different
# TODO: in the case without limit first sorting can be removed, since thay have no effect
test_query 0 '
    SELECT * FROM ( SELECT x, y FROM (SELECT number AS x, number AS y FROM numbers_mt(1000))
        ORDER BY x DESC, y DESC ) AS t1
    JOIN ( SELECT a, b FROM (SELECT number AS a, number AS b FROM numbers_mt(1000))
        ORDER BY a, b DESC ) AS t2
    ON t1.x = t2.a AND t1.y = t2.b
    SETTINGS max_rows_in_set_to_optimize_join = 0'

test_query 0 '
    SELECT * FROM ( SELECT x, y FROM (SELECT number AS x, number AS y FROM numbers_mt(1000))
        ORDER BY x DESC, y DESC LIMIT 100 ) AS t1
    JOIN ( SELECT a, b FROM (SELECT number AS a, number AS b FROM numbers_mt(1000))
        ORDER BY a DESC, b DESC LIMIT 100 ) AS t2
    ON t1.x = t2.a AND t1.y = t2.b
    SETTINGS max_rows_in_set_to_optimize_join = 0'

$CLICKHOUSE_CLIENT -mn -q "
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS ta1;
DROP TABLE IF EXISTS ta2;
"
