#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This function takes 2 arguments:
# $1 - query id
# $2 - query
function execute_query()
{
  ${CLICKHOUSE_CLIENT} --opentelemetry_start_trace_probability=1 --query_id $1 -q "
      ${2}
  "
}

function execute_query_HTTP() {
    response=$(curl -s -X POST "$CLICKHOUSE_HTTP_URL" \
        --data-urlencode "query=$2" \
        --header "X-ClickHouse-Query-Id: $1")
}

# For some queries, it's not possible to know how many bytes/rows are read when tests are executed on CI,
# so we only to check the db.statement only
function check_query_span_query_only()
{
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS;
    SELECT attribute['db.statement']       as query
    FROM system.opentelemetry_span_log
    WHERE finish_date                      >= yesterday()
    AND   operation_name                   = 'query'
    AND   attribute['clickhouse.query_id'] = '${1}'
    Format JSONEachRow
    ;"
}

function check_query_span()
{
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS;
    SELECT attribute['db.statement']             as query,
           attribute['clickhouse.read_rows']     as read_rows,
           attribute['clickhouse.written_rows']  as written_rows
    FROM system.opentelemetry_span_log
    WHERE finish_date                      >= yesterday()
    AND   operation_name                   = 'query'
    AND   attribute['clickhouse.query_id'] = '${1}'
    Format JSONEachRow
    ;"
}

function check_http_attributes()
{
result=$(${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS;
    SELECT attribute['http.referer'],
           attribute['http.user.agent'],
           attribute['http.method']
    FROM system.opentelemetry_span_log
    WHERE finish_date                      >= yesterday()
    AND   operation_name                   = 'query'
    AND   attribute['clickhouse.query_id'] = '${1}'
    FORMAT JSONEachRow;
  ")
    result_json=$(check_http_attributes_in_result "$result")
    echo "$result_json"
}

check_http_attributes_HTTP() {
    local query_id="$1"
    result=$(curl -s -X POST "$CLICKHOUSE_URL" \
        --data-urlencode "query=SYSTEM FLUSH LOGS; 
        SELECT attribute['http.referer'], attribute['http.user.agent'], attribute['http.method'] 
        FROM system.opentelemetry_span_log 
        WHERE finish_date >= yesterday() 
        AND operation_name = 'query' 
        AND attribute['clickhouse.query_id'] = '$query_id' 
        FORMAT JSONEachRow")
    result_json=$(check_http_attributes_in_result "$result")
    echo "$result_json"
}

check_http_attributes_in_result() {
    local result="$1"
    local referer="not found"
    local agent="not found"
    local method="not found"
    if [[ $result == *"http.referer"* ]]; then
        referer="present"
    fi
    if [[ $result == *"http.user.agent"* ]]; then
        agent="present"
    fi
    if [[ $result == *"http.method"* ]]; then
        method="present"
    fi
    echo "{\"http.referer\":\"$referer\",\"http.user.agent\":\"$agent\",\"http.method\":\"$method\"}"
}
#
# Set up
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.opentelemetry_test;
CREATE TABLE ${CLICKHOUSE_DATABASE}.opentelemetry_test (id UInt64) Engine=MergeTree Order By id;
"

# test 1, a query that has special path in the code
# Format Null is used to make sure no output is generated so that it won't pollute the reference file
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()");
execute_query $query_id 'show processlist format Null'
check_query_span_query_only "$query_id"

# test 2, a normal show command
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()");
execute_query $query_id 'show databases format Null'
check_query_span_query_only "$query_id"

# test 3, a normal insert query on local table
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()");
execute_query $query_id 'insert into opentelemetry_test values(1)(2)(3)'
check_query_span "$query_id"

# test 4, a normal select query
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()");
execute_query $query_id 'select * from opentelemetry_test format Null'
check_query_span $query_id

# Test 5: Executes a TCP SELECT query and checks for http attributes in OpenTelemetry spans.
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()");
execute_query $query_id 'select * from opentelemetry_test format Null'
check_http_attributes $query_id

# Test 6: Executes a HTTP SELECT query and checks for HTTP attributes in OpenTelemetry spans.
query_id=$(curl -s -X POST "$CLICKHOUSE_URL" \
    --data-urlencode "query=SELECT generateUUIDv4() FROM default.some_table" | tr -d '\r\n')
execute_query_HTTP "$query_id" 'SELECT * FROM opentelemetry_test FORMAT Null'
check_http_attributes_HTTP "$query_id"
#
# Tear down
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.opentelemetry_test;
"
