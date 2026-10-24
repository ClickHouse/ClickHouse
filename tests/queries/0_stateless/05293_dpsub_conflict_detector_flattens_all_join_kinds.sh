#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With a conflict detector, DPsub reorders every kind of join the detector models, so the whole
# join tree has to reach it as one group of tables: both sides of an outer or semi/anti join, and
# `FULL` joins too. Without a detector only the preserved side of an outer join is flattened, and a
# `RIGHT`/`FULL`/`RIGHT SEMI`/`RIGHT ANTI` join at the top, or a `FULL` join anywhere, cut the tree
# into groups of two tables that can only be swapped.
#
# For each shape print the sizes of the groups the join order optimizer was given and the
# algorithms that planned them, and check the result against greedy without a detector.

# Pinned because the harness randomizes them and they change how the groups are formed.
SETTINGS=(
    --query_plan_optimize_join_order_limit 10
    --query_plan_optimize_join_order_randomize 0
    --query_plan_convert_outer_join_to_inner_join 0
    --automatic_parallel_replicas_mode 0
    --join_use_nulls 0
)
DPSUB=(--query_plan_optimize_join_order_algorithm 'dpsub,greedy' --query_plan_optimize_join_order_conflict_detector 'c')
GREEDY=(--query_plan_optimize_join_order_algorithm 'greedy' --query_plan_optimize_join_order_conflict_detector '')

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS table_1;
    DROP TABLE IF EXISTS table_2;
    DROP TABLE IF EXISTS table_3;
    CREATE TABLE table_1 (a Nullable(Int32), p Int32) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE table_2 (a Nullable(Int32), b Nullable(Int32), q Int32) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE table_3 (b Nullable(Int32), r Int32) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO table_1 SELECT if(number % 7 = 0, NULL, number % 10), number FROM numbers(300);
    INSERT INTO table_2 SELECT if(number % 5 = 0, NULL, number % 8), number % 6, number FROM numbers(40);
    INSERT INTO table_3 SELECT if(number % 4 = 0, NULL, number % 5), number FROM numbers(20);
"

check()
{
    local from="$1" output="$2"
    local query="SELECT count(), sum(cityHash64($output)) FROM $from"
    echo "-- $from"
    $CLICKHOUSE_CLIENT "${SETTINGS[@]}" "${DPSUB[@]}" --send_logs_level trace -q "$query FORMAT Null" 2>&1 \
        | grep -oE 'query graph with [0-9]+ relations|Solving join order using [A-Z]+ algorithm' \
        | sed -E 's/query graph with ([0-9]+) relations/group of \1/; s/Solving join order using ([A-Z]+) algorithm/planned by \1/'
    local reordered expected
    reordered=$($CLICKHOUSE_CLIENT "${SETTINGS[@]}" "${DPSUB[@]}" -q "$query")
    expected=$($CLICKHOUSE_CLIENT "${SETTINGS[@]}" "${GREEDY[@]}" -q "$query")
    [ "$reordered" = "$expected" ] && echo "same result" || echo "DIFFERENT RESULT: $reordered vs $expected"
}

check "table_1 INNER JOIN table_2 ON table_1.a = table_2.a RIGHT JOIN table_3 ON table_2.b = table_3.b" "table_3.r"
check "table_1 INNER JOIN table_2 ON table_1.a = table_2.a FULL JOIN table_3 ON table_2.b = table_3.b" "table_1.p, table_3.r"
check "table_1 FULL JOIN table_2 ON table_1.a = table_2.a INNER JOIN table_3 ON table_2.b = table_3.b" "table_1.p, table_3.r"
check "table_1 LEFT JOIN table_2 ON table_1.a = table_2.a RIGHT SEMI JOIN table_3 ON table_2.b = table_3.b" "table_3.r"
check "table_1 INNER JOIN table_2 ON table_1.a = table_2.a RIGHT ANTI JOIN table_3 ON table_2.b = table_3.b" "table_3.r"
check "table_1 LEFT SEMI JOIN table_2 ON table_1.a = table_2.a FULL JOIN table_3 ON table_1.p % 6 = table_3.b" "table_1.p, table_3.r"

# A cross product among the plain joins of a group holding a semi join is still a cross product.
echo "-- join kinds of table_1 CROSS JOIN table_3 LEFT SEMI JOIN table_2"
$CLICKHOUSE_CLIENT "${SETTINGS[@]}" "${DPSUB[@]}" -q "
    SELECT extract(explain, 'Type: [a-z]+') AS kind FROM (
        EXPLAIN actions = 1 SELECT count() FROM table_1 CROSS JOIN table_3 LEFT SEMI JOIN table_2 ON table_1.a = table_2.a
    ) WHERE explain LIKE '%Type:%' ORDER BY kind"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE table_1;
    DROP TABLE table_2;
    DROP TABLE table_3;
"
