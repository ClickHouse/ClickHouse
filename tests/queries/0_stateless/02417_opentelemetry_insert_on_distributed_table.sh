#!/usr/bin/env bash
# Tags: no-fasttest, distributed, long, no-async-insert
# no-async-insert: sync and async inserts are tested

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# This function takes 4 arguments:
# $1 - OpenTelemetry Trace Id
# $2 - value of distributed_foreground_insert
# $3 - value of prefer_localhost_replica
# $4 - a String that helps to debug
function insert()
{
    echo "INSERT INTO ${CLICKHOUSE_DATABASE}.dist_opentelemetry SETTINGS distributed_foreground_insert=$2, prefer_localhost_replica=$3 VALUES(1),(2)" |
        ${CLICKHOUSE_CURL} \
            -X POST \
            -H "traceparent: 00-$1-5150000000000515-01" \
            -H "tracestate: $4" \
            "${CLICKHOUSE_URL}" \
            --data @-

    # disable probabilistic tracing to avoid stealing the trace context
    ${CLICKHOUSE_CLIENT} --opentelemetry_start_trace_probability=0 -q "SYSTEM FLUSH DISTRIBUTED ${CLICKHOUSE_DATABASE}.dist_opentelemetry"
}

function check_span()
{
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS opentelemetry_span_log;

    SELECT operation_name,
           attribute['clickhouse.cluster'] AS cluster,
           attribute['clickhouse.shard_num'] AS shard,
           attribute['clickhouse.rows'] AS rows,
           attribute['clickhouse.bytes'] AS bytes
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday()
    AND   lower(hex(trace_id))                = '${1}'
    AND   attribute['clickhouse.distributed'] = '${CLICKHOUSE_DATABASE}.dist_opentelemetry'
    AND   attribute['clickhouse.remote']      = '${CLICKHOUSE_DATABASE}.local_opentelemetry'
    ORDER BY attribute['clickhouse.shard_num']
    Format JSONEachRow
    ;"
}

#
# $1 - OpenTelemetry Trace Id
# $2 - span kind to count
# $3 - expected number of spans of that kind
#
# For a distributed INSERT the counted spans come from several scopes and are all
# enqueued into the async opentelemetry_span_log. The racy ones are the remote-shard
# SERVER spans: each is written from that shard's TCPHandler TracingContextHolder on
# its own thread, which can still be finishing after the initiator's client call
# returned. So counting once can read the log before the last span lands and return a
# short count (e.g. 2 instead of 3) under slow builds. Poll until the expected count
# is reached instead.
function check_span_kind()
{
    local count=0
    for _ in {1..30}; do
        count=$(${CLICKHOUSE_CLIENT} -q "
            SYSTEM FLUSH LOGS opentelemetry_span_log;

            SELECT count()
            FROM system.opentelemetry_span_log
            WHERE finish_date >= yesterday()
            AND   lower(hex(trace_id))           = '${1}'
            AND   kind                           = '${2}'
            ;")
        # Retry only while we got a numeric count below the expected value; a
        # non-numeric result (client/flush error) breaks out and is reported as-is
        # so the failure surfaces immediately instead of looping for 30s.
        [[ "$count" =~ ^[0-9]+$ ]] || break
        [[ "$count" -ge "$3" ]] && break
        sleep 1
    done
    # Print the final count. If a span is genuinely missing the loop times out
    # and this still reports the short count, so the test keeps catching it.
    echo "$count"
}


#
# Prepare tables for tests
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.local_opentelemetry;

CREATE TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry  (key UInt64) Engine=Distributed('test_cluster_two_shards_localhost', ${CLICKHOUSE_DATABASE}, local_opentelemetry, key % 2);
CREATE TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry (key UInt64) Engine=MergeTree ORDER BY key;

SYSTEM STOP DISTRIBUTED SENDS ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
"

#
# test1
#
echo "===1==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 0 1 "async-insert-writeToLocal"
check_span $trace_id
# 1 HTTP SERVER spans
check_span_kind $trace_id 'SERVER' 1

#
# test2
#
echo "===2==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 0 0 "async-insert-writeToRemote"
check_span $trace_id
# 3 SERVER spans, 1 for HTTP, 2 for TCP
check_span_kind $trace_id 'SERVER' 3
# 2 CLIENT spans
check_span_kind $trace_id 'CLIENT' 2

#
# test3
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 1 1  "sync-insert-writeToLocal"
echo "===3==="
check_span $trace_id
# 1 HTTP SERVER spans
check_span_kind $trace_id 'SERVER' 1

#
# test4
#
echo "===4==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 1 0  "sync-insert-writeToRemote"
check_span $trace_id
# 3 SERVER spans, 1 for HTTP, 2 for TCP
check_span_kind $trace_id 'SERVER' 3
# 2 CLIENT spans
check_span_kind $trace_id 'CLIENT' 2

#
# Cleanup
#
${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry;
"
