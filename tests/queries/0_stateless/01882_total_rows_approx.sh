#!/usr/bin/env bash

# Check that total_rows_approx (via system.processes) includes all rows from
# all parts at the query start.
#
# At some point total_rows_approx was accounted only when the query starts
# reading the part, and so total_rows_approx wasn't reliable, even for simple
# SELECT FROM MergeTree()
# It was fixed by take total_rows_approx into account as soon as possible.
#
# To check total_rows_approx this query starts the query in background,
# that sleep's 1 second for each part, and by using max_threads=1 the query
# reads parts sequentially and sleeps 1 second between parts.
# Also the test spawns background process to check total_rows_approx for this
# query.
# It checks multiple times since at first few iterations the query may not
# start yet (since there are 3 excessive sleep calls - 1 for primary key
# analysis and 2 for partition pruning), and get only last 5 total_rows_approx
# rows (one row is not enough since when the query finishes total_rows_approx
# will be set to 10 anyway, regardless proper accounting).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists data_01882"
$CLICKHOUSE_CLIENT -q "create table data_01882 (key Int) Engine=MergeTree() partition by key order by key as select * from numbers(10)"
QUERY_ID="$CLICKHOUSE_TEST_NAME-$(tr -cd '[:lower:]' < /dev/urandom | head -c10)"

function check_background_query()
{
    echo "Waiting for query to be started..."
    while [[ $($CLICKHOUSE_CLIENT --param_query_id="$QUERY_ID" -q 'select count() from system.processes where query_id = {query_id:String}') != 1 ]]; do
        sleep 0.01
    done
    echo "Query started."

    echo "Checking total_rows_approx."
    # check total_rows_approx multiple times
    # (to make test more reliable to what it covers)
    local i=0
    for ((i = 0; i < 20; ++i)); do
        $CLICKHOUSE_CLIENT --param_query_id="$QUERY_ID" -q 'select total_rows_approx from system.processes where query_id = {query_id:String}'
        (( ++i ))
        sleep 1
    done | tail -n5
}
check_background_query &

# this query will sleep 10 seconds in total, 1 seconds for each part (10 parts).
$CLICKHOUSE_CLIENT -q "select *, sleepEachRow(1) from data_01882" --max_threads=1 --format Null --query_id="$QUERY_ID" --max_block_size=1

wait

$CLICKHOUSE_CLIENT -q "drop table data_01882"
