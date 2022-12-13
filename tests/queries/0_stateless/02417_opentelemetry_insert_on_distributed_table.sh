#!/usr/bin/env bash
# Tags: no-fasttest, distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# This function takes 4 arguments:
# $1 - OpenTelemetry Trace Id
# $2 - value of insert_distributed_sync
# $3 - value of prefer_localhost_replica
# $4 - a String that helps to debug
function insert()
{
    echo "INSERT INTO ${CLICKHOUSE_DATABASE}.dist_opentelemetry SETTINGS insert_distributed_sync=$2, prefer_localhost_replica=$3 VALUES(1),(2)" |
        ${CLICKHOUSE_CURL} \
            -X POST \
            -H "traceparent: 00-$1-5150000000000515-01" \
            -H "tracestate: $4" \
            "${CLICKHOUSE_URL}" \
            --data @-
}

function check_span()
{
${CLICKHOUSE_CLIENT} -nq "
    SYSTEM FLUSH LOGS;

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
# Prepare tables for tests
#
${CLICKHOUSE_CLIENT} -nq "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.local_opentelemetry;

CREATE TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry  (key UInt64) Engine=Distributed('test_cluster_two_shards_localhost', ${CLICKHOUSE_DATABASE}, local_opentelemetry, key % 2);
CREATE TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry (key UInt64) Engine=MergeTree ORDER BY key;
"

#
# test1
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 0 1 "async-insert-writeToLocal"
check_span $trace_id

#
# test2
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 0 0 "async-insert-writeToRemote"
check_span $trace_id

#
# test3
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 1 1  "sync-insert-writeToLocal"
check_span $trace_id

#
# test4
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 1 0  "sync-insert-writeToRemote"
check_span $trace_id

#
# Cleanup
#
${CLICKHOUSE_CLIENT} -nq "
DROP TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry;
"
