#!/usr/bin/env bash
# Tags: distributed

set -ue

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} -nq "
SET distributed_ddl_output_mode = 'none';

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
-- Make sure it's async
SET insert_distributed_sync=0;
INSERT INTO default.dist_opentelemetry SETTINGS opentelemetry_start_trace_probability=1 VALUES(1),(2);
"

# Wait complete of ASYNC INSERT on distributed table
wait

# Check log
${CLICKHOUSE_CLIENT} -nq "
-- Flush opentelemetry span log on all nodes
SET distributed_ddl_output_mode = 'none';
SYSTEM FLUSH LOGS ON CLUSTER test_cluster_two_shards;

-- Above INSERT will insert data to two shards respectively, so there will be two spans generated
SELECT attribute FROM cluster('test_cluster_two_shards', system, opentelemetry_span_log) WHERE operation_name like '%writeToLocal%';
SELECT attribute FROM cluster('test_cluster_two_shards', system, opentelemetry_span_log) WHERE operation_name like '%processFile%';
"

#
# INSERT SYNC test
# Do test with opentelemetry enabled and in SYNC mode
#
${CLICKHOUSE_CLIENT} -nq "

-- Clear log
SET distributed_ddl_output_mode = 'none';
TRUNCATE TABLE IF EXISTS system.opentelemetry_span_log ON CLUSTER test_cluster_two_shards;

-- Make sure it's SYNC
SET insert_distributed_sync=1;

-- INSERT test
INSERT INTO default.dist_opentelemetry SETTINGS opentelemetry_start_trace_probability=1 VALUES(1),(2);
"

# Check log
${CLICKHOUSE_CLIENT} -nq "
-- Flush opentelemetry span log on all nodes
SET distributed_ddl_output_mode = 'none';
SYSTEM FLUSH LOGS ON CLUSTER test_cluster_two_shards;

-- Above INSERT will insert data to two shards in the same flow, so there should be two spans generated with the same operation name
SELECT attribute FROM cluster('test_cluster_two_shards', system, opentelemetry_span_log) WHERE operation_name like '%runWritingJob%';
"

#
# Cleanup
#
${CLICKHOUSE_CLIENT} -nq "
SET distributed_ddl_output_mode = 'none';
DROP TABLE default.dist_opentelemetry  ON CLUSTER test_cluster_two_shards;
DROP TABLE default.local_opentelemetry ON CLUSTER test_cluster_two_shards;
"
