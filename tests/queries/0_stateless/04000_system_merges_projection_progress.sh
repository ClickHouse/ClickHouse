#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_merge_proj" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_rebuild_proj" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_materialize_proj" 2>/dev/null
}
trap on_exit EXIT

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null

# ============================================================
# Test 1: Merge path — two projections, two pauses
# ============================================================

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_merge_proj (key UInt64, value String)
    ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 64,
        enable_block_number_column = 0,
        enable_block_offset_column = 0
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${CLICKHOUSE_DATABASE}.t_merge_proj"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_merge_proj ADD PROJECTION p_agg (SELECT key, count() GROUP BY key)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_merge_proj ADD PROJECTION p_val (SELECT * ORDER BY value)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_merge_proj SELECT number, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_merge_proj SELECT number + 500, toString(number) FROM numbers(500)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES ${CLICKHOUSE_DATABASE}.t_merge_proj"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${CLICKHOUSE_DATABASE}.t_merge_proj FINAL" &
optimize_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"
echo "--- merge: pause 1 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT current_projection, current_projection_parts_merging, current_projection_parts_remaining,
        arraySort(projections_completed), arraySort(projections_remaining)
    FROM system.merges
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_merge_proj' AND NOT is_mutation
"

${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT merge_task_projection_stage_pause"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"
echo "--- merge: pause 2 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT current_projection, current_projection_parts_merging, current_projection_parts_remaining,
        arraySort(projections_completed), arraySort(projections_remaining)
    FROM system.merges
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_merge_proj' AND NOT is_mutation
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait $optimize_pid

# ============================================================
# Test 2: Rebuild path — ReplacingMergeTree, two projections
# ============================================================

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_rebuild_proj (key UInt64, value String)
    ENGINE = ReplacingMergeTree ORDER BY key
    SETTINGS index_granularity = 64,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        deduplicate_merge_projection_mode = 'rebuild'
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${CLICKHOUSE_DATABASE}.t_rebuild_proj"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_rebuild_proj ADD PROJECTION p_agg (SELECT key, count() GROUP BY key)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_rebuild_proj ADD PROJECTION p_val (SELECT * ORDER BY value)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_rebuild_proj SELECT number, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_rebuild_proj SELECT number + 500, toString(number) FROM numbers(500)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES ${CLICKHOUSE_DATABASE}.t_rebuild_proj"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${CLICKHOUSE_DATABASE}.t_rebuild_proj FINAL" &
optimize_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"
echo "--- rebuild: pause 1 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT current_projection,
        arraySort(projections_completed), arraySort(projections_remaining)
    FROM system.merges
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_rebuild_proj' AND NOT is_mutation
"

${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT merge_task_projection_stage_pause"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"
echo "--- rebuild: pause 2 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT current_projection,
        arraySort(projections_completed), arraySort(projections_remaining)
    FROM system.merges
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_rebuild_proj' AND NOT is_mutation
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait $optimize_pid

# ============================================================
# Test 3: Mutation path — MATERIALIZE PROJECTION
# ============================================================

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_materialize_proj (key UInt64, value String)
    ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 64,
        enable_block_number_column = 0,
        enable_block_offset_column = 0
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${CLICKHOUSE_DATABASE}.t_materialize_proj"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_materialize_proj SELECT number, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_materialize_proj ADD PROJECTION p_agg (SELECT key, count() GROUP BY key)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_materialize_proj ADD PROJECTION p_val (SELECT * ORDER BY value)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES ${CLICKHOUSE_DATABASE}.t_materialize_proj"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_materialize_proj MATERIALIZE PROJECTION p_agg" &
materialize_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"
echo "--- materialize: pause 1 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT current_projection,
        arraySort(projections_completed), arraySort(projections_remaining)
    FROM system.merges
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_materialize_proj' AND is_mutation
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait $materialize_pid
