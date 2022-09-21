#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This function takes following arguments:
# $1 - OpenTelemetry Trace Id
# $2 - Query
# $3 - Query Settings
# $4 - Output device, default is stdout
function execute_query()
{
    if [ -n "${4}" ]; then
        output=$4
    else
        output="/dev/stdout"
    fi

    echo $2 | ${CLICKHOUSE_CURL} \
                -X POST \
                -H "traceparent: 00-$1-5150000000000515-01" \
                -H "tracestate: a\nb cd" \
                "${CLICKHOUSE_URL}?${3}" \
                --data @- \
                > $output
}

# This function takes 3 argument:
# $1 - OpenTelemetry Trace Id
# $2 - Fields
# $3 - operation_name pattern
function check_span()
{
${CLICKHOUSE_CLIENT} -nq "
    SYSTEM FLUSH LOGS;

    SELECT ${2}
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday()
    AND   lower(hex(trace_id)) =    '${1}'
    AND   operation_name       like '${3}'
    ;"
}

#
# Set up
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ddl_test_for_opentelemetry;
"

case_no=1;

#
# normal cases for ALL distributed_ddl_entry_format_version
#
for ddl_version in 1 2 3; do
    # Echo a separator so that the reference file is more clear for reading
    echo "===case ${case_no}===="

    trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
    execute_query $trace_id "CREATE TABLE ddl_test_for_opentelemetry ON CLUSTER test_shard_localhost (id UInt64) Engine=MergeTree ORDER BY id" "distributed_ddl_output_mode=none&distributed_ddl_entry_format_version=${ddl_version}" "/dev/null"

    check_span $trace_id "count()" "HTTPHandler"
    check_span $trace_id "count()" "%DDLWorker::processTask%"
    check_span $trace_id "attribute['clickhouse.cluster']" "%executeDDLQueryOnCluster%"

    # There should be two 'query' spans,
    # one is for the HTTPHandler, the other is for the DDL executing in DDLWorker
    check_span $trace_id "count()" "query"

    # Remove table
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS ddl_test_for_opentelemetry;
    "

    case_no=$(($case_no + 1))
done

#
# an exceptional case, DROP a non-exist table
#
# Echo a separator so that the reference file is more clear for reading
echo "===case ${case_no}===="

# Since this query is supposed to fail, we redirect the error message to /dev/null to discard the error message so that it won't pollute the reference file.
# The exception will be checked in the span log
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
execute_query $trace_id "DROP TABLE ddl_test_for_opentelemetry_non_exist ON CLUSTER test_shard_localhost" "distributed_ddl_output_mode=none" "/dev/null"

check_span $trace_id "count()" "HTTPHandler"
check_span $trace_id "count()" "%DDLWorker::processTask%"
check_span $trace_id "attribute['clickhouse.cluster']" "%executeDDLQueryOnCluster%"

# There should be two 'query' spans,
# one is for the HTTPHandler, the other is for the DDL executing in DDLWorker.
# Both of these two spans contain exception
check_span $trace_id "concat('exception_code=', attribute['clickhouse.exception_code'])" "query"

#
# Tear down
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ddl_test_for_opentelemetry;
"