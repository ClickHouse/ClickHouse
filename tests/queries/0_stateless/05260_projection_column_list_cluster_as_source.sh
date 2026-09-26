#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

# In old-format distributed DDL, the worker resolves AS <source> after the initiator checks the
# query. Both names must contain the runner's database: the worker's current database is `default`,
# and parallel flaky-test retries can otherwise collide on the source table.
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

source_table="${CLICKHOUSE_DATABASE}.t_projection_column_list_source"
copy_table="${CLICKHOUSE_DATABASE}.t_projection_column_list_cluster_copy"
inherited_table="${CLICKHOUSE_DATABASE}.t_projection_column_list_cluster_inherited"
memory_table="${CLICKHOUSE_DATABASE}.t_projection_column_list_cluster_memory"

old_format=(--distributed_ddl_entry_format_version=1)
wait_for_worker=(--distributed_ddl_task_timeout=180 --distributed_ddl_output_mode=throw)

expect_disabled_before_enqueue() {
    local label="$1" query="$2" output
    if output=$(${CLICKHOUSE_CLIENT} "${old_format[@]}" --distributed_ddl_task_timeout=0 \
        --distributed_ddl_output_mode=none --allow_projection_column_list_in_replicated_metadata=0 \
        -q "$query" 2>&1); then
        echo "$label unexpectedly succeeded" >&2
        exit 1
    fi
    if [[ "$output" != *SUPPORT_IS_DISABLED* ]]; then
        echo "$label failed with an unexpected error: $output" >&2
        exit 1
    fi
    echo "$label rejected"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${source_table}
        (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
        ENGINE = MergeTree ORDER BY x"

expect_disabled_before_enqueue copy "
    CREATE TABLE ${copy_table} ON CLUSTER test_shard_localhost AS ${source_table}
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_cluster_copy', 'r1')
        ORDER BY x"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_column_list_cluster_copy'"

expect_disabled_before_enqueue inherited "
    CREATE TABLE ${inherited_table} ON CLUSTER test_shard_localhost AS ${source_table}"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_column_list_cluster_inherited'"

# Memory discards the inherited projection, so this source is safe without the override.
${CLICKHOUSE_CLIENT} "${old_format[@]}" "${wait_for_worker[@]}" \
    --allow_projection_column_list_in_replicated_metadata=0 -q "
    CREATE TABLE ${memory_table} ON CLUSTER test_shard_localhost AS ${source_table} ENGINE = Memory FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_memory'"

# The worker receives neither the initiator's setting nor the expanded source definition in v1.
${CLICKHOUSE_CLIENT} "${old_format[@]}" "${wait_for_worker[@]}" \
    --allow_projection_column_list_in_replicated_metadata=1 -q "
    CREATE TABLE ${copy_table} ON CLUSTER test_shard_localhost AS ${source_table}
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_cluster_copy', 'r1')
        ORDER BY x FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_copy'"

${CLICKHOUSE_CLIENT} "${old_format[@]}" "${wait_for_worker[@]}" \
    --allow_projection_column_list_in_replicated_metadata=1 -q "
    CREATE TABLE ${inherited_table} ON CLUSTER test_shard_localhost AS ${source_table} FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_inherited'"

${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${copy_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${inherited_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${memory_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${source_table}"
