#!/usr/bin/env bash
# Tags: zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The test cases in this file cover DDLs running on both Replicated database engine and non-Replicated database engine.
# Since the processing flow is a little bit different from each other, in order to share same reference file,
# we compare the expected result and actual result by ourselves. See check_span method below for more detail.

# This function takes following arguments:
# $1 - OpenTelemetry Trace Id
# $2 - Query
# $3 - Query Settings
function execute_query()
{
    local trace_id=$1 && shift
    local ddl_version=$1 && shift
    local opts=(
        --opentelemetry-traceparent "00-$trace_id-5150000000000515-01"
        --opentelemetry-tracestate $'a\nb cd'
        --distributed_ddl_output_mode "none"
        --distributed_ddl_entry_format_version "$ddl_version"
    )
    ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@"
}

# This function takes following argument:
# $1 - expected
# $2 - OpenTelemetry Trace Id
# $3 - operation_name pattern
# $4 - extra condition
function check_span()
{
    if [ -n "$4" ]; then
        extra_condition=" AND ${4}"
    else
        extra_condition=""
    fi

    ret=$(${CLICKHOUSE_CLIENT} -q "
        SYSTEM FLUSH LOGS;

        SELECT count()
        FROM system.opentelemetry_span_log
        WHERE finish_date >= yesterday()
        AND   lower(hex(trace_id)) =    '${2}'
        AND   operation_name       like '${3}'
        ${extra_condition};")

    if [ $ret = $1 ]; then
        echo 1
    else
        echo "[operation_name like '${3}' ${extra_condition}]=$ret, expected: ${1}"
        
        # echo the span logs to help analyze        
        ${CLICKHOUSE_CLIENT} -q "
            SELECT operation_name, attribute
            FROM system.opentelemetry_span_log
            WHERE finish_date >= yesterday()
            AND   lower(hex(trace_id)) ='${2}'
            ORDER BY start_time_us
            Format PrettyCompact
        "
    fi
}

#
# Set up
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.ddl_test_for_opentelemetry;
"

# Support Replicated database engine
cluster_name=$($CLICKHOUSE_CLIENT -q "select if(engine = 'Replicated', name, 'test_shard_localhost')  from system.databases where name='$CLICKHOUSE_DATABASE'")

#
# Only format_version 4 enables the tracing
#
for ddl_version in 3 4; do
    # Echo a separator so that the reference file is more clear for reading
    echo "===ddl_format_version ${ddl_version}===="

    trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
    execute_query $trace_id $ddl_version -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.ddl_test_for_opentelemetry ON CLUSTER ${cluster_name} (id UInt64) Engine=MergeTree ORDER BY id"

    check_span 1 $trace_id "TCPHandler"

    if [ $cluster_name = "test_shard_localhost" ]; then
        check_span 1 $trace_id "%executeDDLQueryOnCluster%" "attribute['clickhouse.cluster']='${cluster_name}'"
    else
        check_span 1 $trace_id "%tryEnqueueAndExecuteEntry%" "attribute['clickhouse.cluster']='${cluster_name}'"
    fi
    
    if [ $cluster_name = "test_shard_localhost" ]; then
        # The tracing is only enabled when entry format version is 4
        if [ $ddl_version = "4" ]; then
            expected=1
        else
            expected=0
        fi
    else
        # For Replicated database engine, the tracing is always enabled because it calls DDLWorker::processTask directly
        expected=1
    fi
    check_span $expected $trace_id "%DDLWorker::processTask%"
    
    # For queries that tracing are enabled(format version is 4 or Replicated database engine), there should be two 'query' spans,
    # one is for the TCPHandler, the other is for the DDL executing in DDLWorker.
    #
    # For other format, there should be only one 'query' span
    if [ $cluster_name = "test_shard_localhost" ]; then
        if [ $ddl_version = "4" ]; then
            expected=2
        else
            expected=1
        fi
    else
        expected=2
    fi
    check_span $expected $trace_id "query"

    # Remove table
    # Under Replicated database engine, the DDL is executed as ON CLUSTER DDL, so distributed_ddl_output_mode is needed to supress output
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none -q "
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.ddl_test_for_opentelemetry;
    "
done

#
# an exceptional case, DROP a non-exist table
#
# Echo a separator so that the reference file is more clear for reading
echo "===exception===="

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
execute_query $trace_id 4 -q "DROP TABLE ${CLICKHOUSE_DATABASE}.ddl_test_for_opentelemetry_non_exist ON CLUSTER ${cluster_name}" 2>&1 | grep 'DB::Exception ' | grep -Fv "UNKNOWN_TABLE"

check_span 1 $trace_id "TCPHandler"

if [ $cluster_name = "test_shard_localhost" ]; then
    expected=1
else
    # For Replicated database it will fail on initiator before enqueueing distributed DDL
    expected=0
fi
check_span $expected $trace_id "%executeDDLQueryOnCluster%" "attribute['clickhouse.cluster']='${cluster_name}' AND kind = 'PRODUCER'"
check_span $expected $trace_id "%DDLWorker::processTask%" "kind = 'CONSUMER'"

if [ $cluster_name = "test_shard_localhost" ]; then
    # There should be two 'query' spans, one is for the TCPHandler, the other is for the DDL executing in DDLWorker.
    # Both of these two spans contain exception
    expected=2
else
    # For Replicated database, there should only one query span
    expected=1
fi
# We don't case about the exact value of exception_code, just check it's there.
check_span $expected $trace_id "query" "attribute['clickhouse.exception_code']<>''"
