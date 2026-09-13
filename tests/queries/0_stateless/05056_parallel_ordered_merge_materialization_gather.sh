#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: distributed planning requires the analyzer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table_name="parallel_ordered_merge_materialization_gather"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_gather"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table_name}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table_name} (partition_key UInt32, value UInt64)
    ENGINE = MergeTree
    ORDER BY (partition_key, value)
    SETTINGS index_granularity = 64"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${table_name}"

for offset in 0 4096 8192; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO ${table_name}
        SELECT number % 32, number + ${offset}
        FROM numbers(4096)"
done

# The sorted gather merges the ordered worker buckets on the coordinator. Its `GatherReceive`
# pipeline must use parallel materialization and restore the original output block order.
${CLICKHOUSE_CLIENT} --query_id "${query_id}" --query "
    SELECT
        partition_key,
        value,
        sum(value) OVER (PARTITION BY partition_key ORDER BY value)
    FROM ${table_name}
    ORDER BY partition_key, value
    FORMAT Null
    SETTINGS
        make_distributed_plan = 1,
        distributed_plan_execute_locally = 1,
        enable_parallel_replicas = 0,
        distributed_plan_max_rows_to_broadcast = 0,
        distributed_plan_default_shuffle_join_bucket_count = 4,
        distributed_plan_default_reader_bucket_count = 4,
        enable_join_runtime_filters = 0,
        optimize_read_in_order = 0,
        max_threads = 4,
        max_parallel_ordered_merge_materialization_threads = 4,
        log_processors_profiles = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(plan_step_name = 'GatherReceive' AND name = 'MaterializeMergedDataTransform') > 0,
        countIf(plan_step_name = 'GatherReceive' AND name = 'SortChunksBySequenceNumber') > 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '${query_id}'
      AND event_date >= yesterday()
    SETTINGS make_distributed_plan = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table_name}"
