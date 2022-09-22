#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This function takes 2 arguments:
# $1 - trace id
# $2 - query
function execute_query()
{
    ${CLICKHOUSE_CLIENT} --opentelemetry-traceparent 00-$1-5150000000000525-01 --opentelemetry_trace_processors 1 -nq "
        ${2}
    "
}

function check_mv_span()
{
${CLICKHOUSE_CLIENT} -nq "
    SYSTEM FLUSH LOGS;
    SELECT splitByChar('.', attribute['clickhouse.source'])[2] as source,
           splitByChar('.', attribute['clickhouse.view'])[2]   as view,
           splitByChar('.', attribute['clickhouse.target'])[2] as target,
           attribute['clickhouse.read_rows']                   as readRows,
           attribute['clickhouse.written_rows']                as writtenRows,
           attribute['clickhouse.parts']                       as parts
    FROM system.opentelemetry_span_log
    WHERE finish_date                      >= yesterday()
    AND   operation_name                   = 'MaterializedView'
    AND   lower(hex(trace_id))             = '${1}'
    ORDER BY target
    Format JSONEachRow
    ;"
}

#
# Set up 2 MVs to different target tables from same source
#
${CLICKHOUSE_CLIENT} -nq "
CREATE TABLE IF NOT EXISTS opentelemetry_mv_source (id UInt64) Engine=MergeTree Order By id;
CREATE TABLE IF NOT EXISTS opentelemetry_mv_target1 (id UInt64) Engine=MergeTree ORDER BY id;
CREATE TABLE IF NOT EXISTS opentelemetry_mv_target2 (id UInt64) Engine=MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW IF NOT EXISTS opentelemetry_mv1 TO opentelemetry_mv_target1 AS SELECT id FROM opentelemetry_mv_source;
CREATE MATERIALIZED VIEW IF NOT EXISTS opentelemetry_mv2 TO opentelemetry_mv_target2 AS SELECT id FROM opentelemetry_mv_source;
"

#
# test 1, INSERT in sequential mode
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
execute_query $trace_id "INSERT INTO opentelemetry_mv_source VALUES (1)"
check_mv_span "$trace_id"

#
# test 2, INSERT in parallel mode
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
execute_query $trace_id "INSERT INTO opentelemetry_mv_source SETTINGS parallel_view_processing=1 VALUES (2)"
check_mv_span "$trace_id"

#
# Check if the target tables have correct value
# We INSERT twice, for each target table, there should be 2 rows
#
echo "===Check target table==="
${CLICKHOUSE_CLIENT} -nq "
    SELECT id FROM opentelemetry_mv_target1 ORDER BY id;
    SELECT id FROM opentelemetry_mv_target2 ORDER BY id;
";

#
# Tear down
#
${CLICKHOUSE_CLIENT} -nq "
DROP TABLE IF EXISTS opentelemetry_mv1;
DROP TABLE IF EXISTS opentelemetry_mv2;
DROP TABLE IF EXISTS opentelemetry_mv_target1;
DROP TABLE IF EXISTS opentelemetry_mv_target2;
DROP TABLE IF EXISTS opentelemetry_mv_source;
"