#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest
# no-random-settings - HTTP interface can override the setting "query_plan_join_swap_table"
# no-fasttest - 'countmin' sketches need a 3rd party library

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

export SETTINGS="allow_experimental_analyzer=1&query_plan_join_swap_table=auto&allow_experimental_statistics=1&query_plan_join_swap_table_use_statistics=1"

function run_query
{
    $CLICKHOUSE_CURL -sS -d "${1}" "$CLICKHOUSE_URL&$SETTINGS"
}

function run_test_case
{
    CONDITION="${1}"
    QUERY_PREFIX="select * from"
    QUERY_SUFFIX="where t_left.id = t_right.id and ${CONDITION}"
    echo "*** Test case - ${CONDITION} ***"
    for join_order in "t_left, t_right" "t_right, t_left"
    do
        echo "Initial join order - ${join_order}"
        echo -n "Real join order - "
        run_query "explain ${QUERY_PREFIX} ${join_order} ${QUERY_SUFFIX}" 1 | grep "ReadFromMergeTree" | awk '{print $2}' | xargs echo -n
        echo
    done
}

run_query "create table t_left (id UInt64, x UInt64) order by id"
run_query "create table t_right (id UInt64, y UInt64) order by id"
run_query "insert into t_left select number, number * 10 from numbers(1e5)"
run_query "insert into t_right select number, number % 5 from numbers(1e4)"

echo "TOTAL ROWS IN t_left = 100_000, t_right = 10_000"
echo

echo "=== DUMMY ESTIMATOR ==="
run_test_case "x < 100" # Expected estimation t_left = 50_000, t_right = 10_000
run_test_case "x > 100" # Expected estimation t_left = 50_000, t_right = 10_000
run_test_case "x > 100 and x > 200 and x > 300 and x > 400" # Expected estimation t_left = 6_250, t_right = 10_000
run_test_case "x = 100" # Expected estimation t_left = 1_000, t_right = 10_000
run_test_case "y = 0" # Expected estimation t_left = 100_000, t_right = 100
echo

echo "=== MinMax STATISTICS ==="
run_query "alter table t_left add statistics x type minmax"
run_query "alter table t_left materialize statistics x"
wait_for_all_mutations "t_left"

run_test_case "x < 100" # Expected estimation t_left ~= 10, t_right = 10_000
run_test_case "x > 100" # Expected estimation t_left ~= 99_990, t_right = 10_000
run_test_case "x = 100" # Expected estimation t_left = 1_000, t_right = 10_000
echo

echo "=== TDigest STATISTICS ==="
run_query "alter table t_left modify statistics x type tdigest"
run_query "alter table t_left materialize statistics x"
wait_for_all_mutations "t_left"

run_test_case "x < 100" # Expected estimation t_left ~= 10, t_right = 10_000
run_test_case "x > 100" # Expected estimation t_left ~= 99_990, t_right = 10_000
run_test_case "x = 100" # Expected estimation t_left = 1_000, t_right = 10_000
echo

echo "=== CountMinSketch STATISTICS ==="
run_query "alter table t_left modify statistics x type countmin"
run_query "alter table t_left materialize statistics x"
wait_for_all_mutations "t_left"

run_test_case "x > 100" # Expected estimation t_left = 50_000, t_right = 10_000
run_test_case "x = 100" # Expected estimation t_left ~= 1, t_right = 10_000
run_test_case "y = 0" # Expected estimation t_left = 100_000, t_right = 100
echo

echo "=== TDigest AND Uniq STATISTICS ==="
run_query "alter table t_left modify statistics x type tdigest, uniq"
run_query "alter table t_left materialize statistics x"
wait_for_all_mutations "t_left"

run_test_case "x < 100" # Expected estimation t_left ~= 10, t_right = 10_000
run_test_case "x > 100" # Expected estimation t_left ~= 99_990, t_right = 10_000
run_test_case "x = 100" # Expected estimation t_left ~= 1, t_right = 10_000
run_test_case "y = 0" # Expected estimation t_left = 100_000, t_right = 100
echo

run_query "truncate table t_left"
run_query "truncate table t_right"
run_query "insert into t_left select number, number * 10 from numbers(1e3)"
run_query "insert into t_right select number, number % 10000 from numbers(1e6)"

echo "TOTAL ROWS IN t_left = 1_000, t_right = 1_000_000"
echo

echo "=== MinMax AND CountMinSketch STATISTICS ON t_right TABLE  ==="
run_query "alter table t_right add statistics y type minmax, countmin"
run_query "alter table t_right materialize statistics y"
wait_for_all_mutations "t_right"

run_test_case "y < 51" # Expected estimation t_left = 1_000, t_right ~= 5_000
run_test_case "y < 51 and y > 49" # Expected estimation t_left = 1_000, t_right ~= 100
run_test_case "y = 100" # Expected estimation t_left = 1_000, t_right ~= 100
run_test_case "y > 10 and y > 20 and y > 15" # Expected estimation t_left = 1_000, t_right ~= 998_000
run_test_case "y > 10 and y < 1000 and y < 51 and y > 49" # Expected estimation t_left = 1_000, t_right ~= 100
echo

echo "=== TEST WITH CASTS ==="
run_test_case "y < '51'" # Expected estimation t_left = 1_000, t_right ~= 5_000
run_test_case "y < '51' and y > '49'" # Expected estimation t_left = 1_000, t_right ~= 100
run_test_case "y = '100'" # Expected estimation t_left = 1_000, t_right ~= 100
run_test_case "y = CAST(100, 'UInt16')" # Expected estimation t_left = 1_000, t_right ~= 100
