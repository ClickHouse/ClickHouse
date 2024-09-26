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

function check_query_settings()
{
    ${CLICKHOUSE_CLIENT} -q "
        SYSTEM FLUSH LOGS;
        SELECT attribute
        FROM system.opentelemetry_span_log
        WHERE finish_date >= yesterday()
        AND operation_name = 'query'
        AND attribute['clickhouse.query_id'] = '${1}'
        FORMAT JSONEachRow;
    "
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
check_query_settings


#
# Tear down
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.opentelemetry_test;
"
