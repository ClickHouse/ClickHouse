#!/usr/bin/env bash
# Tags: no-fasttest, distributed

set -ue

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
SYSTEM FLUSH LOGS ON CLUSTER test_cluster_two_shards;
TRUNCATE TABLE IF EXISTS system.opentelemetry_span_log ON CLUSTER test_cluster_two_shards;

DROP TABLE IF EXISTS default.dist_opentelemetry ON CLUSTER test_cluster_two_shards;
DROP TABLE IF EXISTS default.local_opentelemetry ON CLUSTER test_cluster_two_shards;

CREATE TABLE default.dist_opentelemetry  ON CLUSTER test_cluster_two_shards (key UInt64) Engine=Distributed('test_cluster_two_shards', default, local_opentelemetry, key % 2);
CREATE TABLE default.local_opentelemetry ON CLUSTER test_cluster_two_shards (key UInt64) Engine=MergeTree ORDER BY key;
"

#
# INSERT ASYNC test
# Do test with opentelemetry enabled
#
${CLICKHOUSE_CLIENT} -nq "
INSERT INTO default.dist_opentelemetry SETTINGS opentelemetry_start_trace_probability=1, insert_distributed_sync=0 VALUES(1),(2);
"

# Check log
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
-- Make sure INSERT on distributed finishes
SYSTEM FLUSH DISTRIBUTED default.dist_opentelemetry ON CLUSTER test_cluster_two_shards;

-- Make sure opentelemetry span log flushed
SYSTEM FLUSH LOGS ON CLUSTER test_cluster_two_shards;

-- Above INSERT will insert data to two shards respectively, so there will be two spans generated
SELECT attribute FROM cluster('test_cluster_two_shards', system, opentelemetry_span_log) WHERE operation_name like '%writeToLocal%';
SELECT attribute FROM cluster('test_cluster_two_shards', system, opentelemetry_span_log) WHERE operation_name like '%processFile%';
"

#
# INSERT SYNC test
# Do test with opentelemetry enabled and in SYNC mode
#
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
-- Clear log
TRUNCATE TABLE IF EXISTS system.opentelemetry_span_log ON CLUSTER test_cluster_two_shards;

INSERT INTO default.dist_opentelemetry SETTINGS opentelemetry_start_trace_probability=1, insert_distributed_sync=1 VALUES(1),(2);
"

# Check log
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
SYSTEM FLUSH LOGS ON CLUSTER test_cluster_two_shards;

-- Above INSERT will insert data to two shards in the same flow, so there should be two spans generated with the same operation name
SELECT attribute FROM cluster('test_cluster_two_shards', system, opentelemetry_span_log) WHERE operation_name like '%runWritingJob%';
"

#
# Cleanup
#
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -nq "
DROP TABLE default.dist_opentelemetry  ON CLUSTER test_cluster_two_shards;
DROP TABLE default.local_opentelemetry ON CLUSTER test_cluster_two_shards;
"
