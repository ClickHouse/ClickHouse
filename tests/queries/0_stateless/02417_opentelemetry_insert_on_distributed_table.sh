#!/usr/bin/env bash
# Tags: no-fasttest, distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function insert()
{
    echo "INSERT INTO ${CLICKHOUSE_DATABASE}.dist_opentelemetry SETTINGS insert_distributed_sync=$2 VALUES(1),(2)" |
        ${CLICKHOUSE_CURL} \
            -X POST \
            -H "traceparent: 00-$1-5150000000000515-01" \
            -H "tracestate: some custom state" \
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
    Format JSONEachRow
    ;"
}


#
# Prepare tables for tests
#
${CLICKHOUSE_CLIENT} -nq "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.local_opentelemetry;

CREATE TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry  (key UInt64) Engine=Distributed('test_cluster_two_shards', ${CLICKHOUSE_DATABASE}, local_opentelemetry, key % 2);
CREATE TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry (key UInt64) Engine=MergeTree ORDER BY key;
"

#
# ASYNC INSERT test with opentelemetry enabled
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 0
check_span $trace_id

#
# SYNC INSERT SYNC test with opentelemetry enabled
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
insert $trace_id 1
check_span $trace_id

#
# Cleanup
#
${CLICKHOUSE_CLIENT} -nq "
DROP TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry;
"
