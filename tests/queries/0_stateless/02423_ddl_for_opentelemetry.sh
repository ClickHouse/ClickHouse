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
# $4 - extra condition
function check_span()
{
    if [ -n "$4" ]; then
        extra_condition=" AND ${4}"
    else
        extra_condition=""
    fi

${CLICKHOUSE_CLIENT} -nq "
    SYSTEM FLUSH LOGS;

    SELECT ${2}
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday()
    AND   lower(hex(trace_id)) =    '${1}'
    AND   operation_name       like '${3}'
    ${extra_condition}
    Format TSKV
    ;"
}

#
# Set up
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ddl_test_for_opentelemetry;
"

# Support Replicated database engine
cluster_name=$($CLICKHOUSE_CLIENT -q "select if(engine = 'Replicated', name, 'test_shard_localhost')  from system.databases where name='$CLICKHOUSE_DATABASE'")

#
# Normal cases for ALL distributed_ddl_entry_format_version.
# Only format_version 4 enables the tracing
#
for ddl_version in 1 2 3 4; do
    # Echo a separator so that the reference file is more clear for reading
    echo "===ddl_format_version ${ddl_version}===="

    trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
    execute_query $trace_id "CREATE TABLE ddl_test_for_opentelemetry ON CLUSTER ${cluster_name} (id UInt64) Engine=MergeTree ORDER BY id" "distributed_ddl_output_mode=none&distributed_ddl_entry_format_version=${ddl_version}" "/dev/null"

    check_span $trace_id "count() AS httpHandler" "HTTPHandler"
    check_span $trace_id "count() AS executeDDLQueryOnCluster" "%executeDDLQueryOnCluster%" "attribute['clickhouse.cluster']='${cluster_name}'"
    check_span $trace_id "count() AS processTask" "%DDLWorker::processTask%"

    # For format_version 4, there should be two 'query' spans,
    # one is for the HTTPHandler, the other is for the DDL executing in DDLWorker.
    #
    # For other format, there should be only one 'query' span
    #
    check_span $trace_id "count() AS query" "query"

    # Remove table
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS ddl_test_for_opentelemetry;
    "
done

#
# an exceptional case, DROP a non-exist table
#
# Echo a separator so that the reference file is more clear for reading
echo "===exception===="

# Since this query is supposed to fail, we redirect the error message to /dev/null to discard the error message so that it won't pollute the reference file.
# The exception will be checked in the span log
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
execute_query $trace_id "DROP TABLE ddl_test_for_opentelemetry_non_exist ON CLUSTER ${cluster_name}" "distributed_ddl_output_mode=none&distributed_ddl_entry_format_version=4" "/dev/null"

check_span $trace_id "count() AS httpHandler" "HTTPHandler"
check_span $trace_id "count() AS executeDDLQueryOnCluster" "%executeDDLQueryOnCluster%" "attribute['clickhouse.cluster']='${cluster_name}'"
check_span $trace_id "count() AS processTask" "%DDLWorker::processTask%"

# There should be two 'query' spans,
# one is for the HTTPHandler, the other is for the DDL executing in DDLWorker.
# Both of these two spans contain exception
check_span $trace_id "attribute['clickhouse.exception_code'] AS exceptionCode" "query"

#
# Tear down
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ddl_test_for_opentelemetry;
"