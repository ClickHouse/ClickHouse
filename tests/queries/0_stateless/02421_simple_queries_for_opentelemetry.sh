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

# For some queries, it's not possible to know how many bytes/rows are read when tests are executed on CI,
# so we only to check the db.statement only
function check_query_span_query_only()
{
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS opentelemetry_span_log;
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
    SYSTEM FLUSH LOGS opentelemetry_span_log;
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

function check_query_settings()
{
result=$(${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS opentelemetry_span_log;
    SELECT attribute['clickhouse.setting.min_compress_block_size'],
           attribute['clickhouse.setting.max_block_size'],
           attribute['clickhouse.setting.max_execution_time']
    FROM system.opentelemetry_span_log
    WHERE finish_date                      >= yesterday()
    AND   operation_name                   = 'query'
    AND   attribute['clickhouse.query_id'] = '${1}'
    FORMAT JSONEachRow;
  ")

    local min_present="not found"
    local max_present="not found"
    local execution_time_present="not found"

    if [[ $result == *"min_compress_block_size"* ]]; then
       min_present="present"
    fi
    if [[ $result == *"max_block_size"* ]]; then
       max_present="present"
    fi
    if [[ $result == *"max_execution_time"* ]]; then
       execution_time_present="present"
    fi

    echo "{\"min_compress_block_size\":\"$min_present\",\"max_block_size\":\"$max_present\",\"max_execution_time\":\"$execution_time_present\"}"
}

function check_tcp_attributes()
{
  local query_id="$1"
  local result
  local client_version="not found"

  result=$(${CLICKHOUSE_CLIENT} -q "
      SYSTEM FLUSH LOGS opentelemetry_span_log;
      SELECT attribute['client.version']
      FROM system.opentelemetry_span_log
      WHERE finish_date >= yesterday()
      AND operation_name = 'query'
      AND attribute['clickhouse.query_id'] = '${query_id}'
      FORMAT JSONEachRow;
    ")

  if [[ -z "$result" ]]; then
    echo "Error: No result returned from ClickHouse server"
    return 1
  fi
  
  if [[ $result == *"client.version"* ]]; then
    client_version="present"
  fi

  echo "{\"client.version\":\"$client_version\"}"
}

function execute_query_HTTP()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&database=${CLICKHOUSE_DATABASE}&query_id=$1" -d "$2"
}

function check_http_attributes()
{
  local query_id="$1"
  local result
  local referer="not found"
  local agent="not found"
  local method="not found"
  
  result=$(${CLICKHOUSE_CLIENT} -q "
      SYSTEM FLUSH LOGS opentelemetry_span_log;
      SELECT attribute['http.referer'],
             attribute['http.user.agent'],
             attribute['http.method']
      FROM system.opentelemetry_span_log
      WHERE finish_date >= yesterday()
      AND operation_name = 'query'
      AND attribute['clickhouse.query_id'] = '${query_id}'
      FORMAT JSONEachRow;
    ")

  if [[ -z "$result" ]]; then
    echo "Error: No result returned from ClickHouse server"
    return 1
  fi
  
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

# Test 5: A normal select query with a setting
query_id=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4() SETTINGS max_execution_time=3600")
execute_query "$query_id" 'SELECT * FROM opentelemetry_test FORMAT Null'
check_query_span "$query_id"
check_query_settings "$query_id" "max_execution_time"

# Test 6: Executes a TCP SELECT query and checks for http attributes in OpenTelemetry spans.
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()")
execute_query $query_id 'select * from opentelemetry_test format Null'
check_tcp_attributes $query_id

# Test 7: Executes a TCP SELECT query and checks for http attributes in OpenTelemetry spans.
query_id=$(${CLICKHOUSE_CLIENT} -q "select generateUUIDv4()")
execute_query "$query_id" 'set opentelemetry_start_trace_probability=1'
execute_query_HTTP "$query_id" 'select * from opentelemetry_test FORMAT Null'
check_http_attributes "$query_id"
#
# Tear down
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.opentelemetry_test;
"
