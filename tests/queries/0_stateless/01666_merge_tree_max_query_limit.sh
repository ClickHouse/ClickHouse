#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function wait_for_query_to_start()
{
    while [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT sum(read_rows) FROM system.processes WHERE query_id = '$1'") == 0 ]]; do sleep 0.1; done
}

${CLICKHOUSE_CLIENT} --multiline --multiquery --query "
drop table if exists simple;

create table simple (i int, j int) engine = MergeTree order by i
settings index_granularity = 1, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 2;

insert into simple select number, number + 100 from numbers(5000);
"

query_id="long_running_query-$CLICKHOUSE_DATABASE"

echo "Spin up a long running query"
${CLICKHOUSE_CLIENT} --query "select sleepEachRow(0.1) from simple settings max_block_size = 1 format Null" --query_id "$query_id" > /dev/null 2>&1 &
wait_for_query_to_start "$query_id"

# query which reads marks >= min_marks_to_honor_max_concurrent_queries is throttled
echo "Check if another query with some marks to read is throttled"
${CLICKHOUSE_CLIENT} --query "select * from simple" 2> /dev/null;
CODE=$?
[ "$CODE" -ne "202" ] && echo "Expected error code: 202 but got: $CODE" && exit 1;
echo "yes"

# query which reads marks less than min_marks_to_honor_max_concurrent_queries is allowed
echo "Check if another query with less marks to read is passed"
${CLICKHOUSE_CLIENT} --query "select * from simple where i = 0"

# We can modify the settings to take effect for future queries
echo "Modify min_marks_to_honor_max_concurrent_queries to 1"
${CLICKHOUSE_CLIENT} --query "alter table simple modify setting min_marks_to_honor_max_concurrent_queries = 1"

# Now smaller queries are also throttled
echo "Check if another query with less marks to read is throttled"
${CLICKHOUSE_CLIENT} --query "select * from simple where i = 0" 2> /dev/null;
CODE=$?
[ "$CODE" -ne "202" ] && echo "Expected error code: 202 but got: $CODE" && exit 1;
echo "yes"

echo "Modify max_concurrent_queries to 2"
${CLICKHOUSE_CLIENT} --query "alter table simple modify setting max_concurrent_queries = 2"

# Now more queries are accepted
echo "Check if another query is passed"
${CLICKHOUSE_CLIENT} --query "select * from simple where i = 0"

echo "Modify max_concurrent_queries back to 1"
${CLICKHOUSE_CLIENT} --query "alter table simple modify setting max_concurrent_queries = 1"

# Now queries are throttled again
echo "Check if another query with less marks to read is throttled"
${CLICKHOUSE_CLIENT} --query "select * from simple where i = 0" 2> /dev/null;
CODE=$?
[ "$CODE" -ne "202" ] && echo "Expected error code: 202 but got: $CODE" && exit 1;
echo "yes"

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait

${CLICKHOUSE_CLIENT} --multiline --multiquery --query "
drop table simple
"
