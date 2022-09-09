#!/usr/bin/env bash
# Tags: no-fasttest, distributed

set -ue

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.local_opentelemetry;

CREATE TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry  (key UInt64) Engine=Distributed('test_cluster_two_shards', ${CLICKHOUSE_DATABASE}, local_opentelemetry, key % 2);
CREATE TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry (key UInt64) Engine=MergeTree ORDER BY key;
"

#
# INSERT ASYNC test
# Do test with opentelemetry enabled
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
echo "INSERT INTO ${CLICKHOUSE_DATABASE}.dist_opentelemetry SETTINGS insert_distributed_sync=0 VALUES(1),(2)" |
${CLICKHOUSE_CURL} \
    -X POST \
    -H "traceparent: 00-$trace_id-5250000000000525-01" \
    -H "tracestate: some custom state" \
    "${CLICKHOUSE_URL}" \
    --data @-

# Check log
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
-- Make sure INSERT on distributed finishes
SYSTEM FLUSH DISTRIBUTED ${CLICKHOUSE_DATABASE}.dist_opentelemetry;

-- Make sure opentelemetry span log flushed
SYSTEM FLUSH LOGS;

-- Above INSERT will insert data to two shards respectively, so there will be two spans generated
SELECT count() FROM system.opentelemetry_span_log
WHERE lower(hex(trace_id)) = '${trace_id}'
AND   operation_name like '%writeToLocal%'
AND   attribute['clickhouse.shard_num']   = '1'
AND   attribute['clickhouse.cluster']     = 'test_cluster_two_shards'
AND   attribute['clickhouse.distributed'] = '${CLICKHOUSE_DATABASE}.dist_opentelemetry'
AND   attribute['clickhouse.remote']      = '${CLICKHOUSE_DATABASE}.local_opentelemetry'
AND   attribute['clickhouse.rows']        = '1'
AND   attribute['clickhouse.bytes']       = '8'
;

SELECT count() FROM system.opentelemetry_span_log
WHERE lower(hex(trace_id)) = '${trace_id}'
AND   operation_name like '%writeToLocal%'
AND   attribute['clickhouse.shard_num']   = '2'
AND   attribute['clickhouse.cluster']     = 'test_cluster_two_shards'
AND   attribute['clickhouse.distributed'] = '${CLICKHOUSE_DATABASE}.dist_opentelemetry'
AND   attribute['clickhouse.remote']      = '${CLICKHOUSE_DATABASE}.local_opentelemetry'
AND   attribute['clickhouse.rows']        = '1'
AND   attribute['clickhouse.bytes']       = '8'
;

"

#
# INSERT SYNC test
# Do test with opentelemetry enabled and in SYNC mode
#
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))");
echo "INSERT INTO ${CLICKHOUSE_DATABASE}.dist_opentelemetry SETTINGS insert_distributed_sync=1 VALUES(1),(2)" |
${CLICKHOUSE_CURL} \
    -X POST \
    -H "traceparent: 00-$trace_id-5250000000000525-01" \
    -H "tracestate: some custom state" \
    "${CLICKHOUSE_URL}" \
    --data @-

# Check log
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
SYSTEM FLUSH LOGS;

-- Above INSERT will insert data to two shards in the same flow, so there should be two spans generated with the same operation name
SELECT count() FROM system.opentelemetry_span_log
WHERE lower(hex(trace_id)) = '${trace_id}'
AND   operation_name like '%runWritingJob%'
AND   attribute['clickhouse.shard_num']   = '1'
AND   attribute['clickhouse.cluster']     = 'test_cluster_two_shards'
AND   attribute['clickhouse.distributed'] = '${CLICKHOUSE_DATABASE}.dist_opentelemetry'
AND   attribute['clickhouse.remote']      = '${CLICKHOUSE_DATABASE}.local_opentelemetry'
AND   attribute['clickhouse.rows']        = '1'
AND   attribute['clickhouse.bytes']       = '8'
;

SELECT count() FROM system.opentelemetry_span_log
WHERE lower(hex(trace_id)) = '${trace_id}'
AND   operation_name like '%runWritingJob%'
AND   attribute['clickhouse.shard_num']   = '2'
AND   attribute['clickhouse.cluster']     = 'test_cluster_two_shards'
AND   attribute['clickhouse.distributed'] = '${CLICKHOUSE_DATABASE}.dist_opentelemetry'
AND   attribute['clickhouse.remote']      = '${CLICKHOUSE_DATABASE}.local_opentelemetry'
AND   attribute['clickhouse.rows']        = '1'
AND   attribute['clickhouse.bytes']       = '8'
;
"

#
# Cleanup
#
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
DROP TABLE ${CLICKHOUSE_DATABASE}.dist_opentelemetry;
DROP TABLE ${CLICKHOUSE_DATABASE}.local_opentelemetry;
"
