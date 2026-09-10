#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_lazy_partition_vrow"
LOG_COMMENT="${CLICKHOUSE_TEST_UNIQUE_NAME}_lazy_partition_vrow"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE_NAME}"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE_NAME} (ts DateTime, value UInt64)
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(ts)
    ORDER BY ts
    SETTINGS index_granularity = 64"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES ${TABLE_NAME}"

# The first part contains the rows satisfying the limit. The remaining January
# parts should stay unread after their virtual rows are converted from `ts` to
# `toUnixTimestamp(ts)` by the partition-local merge.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO ${TABLE_NAME}
    SELECT toDateTime('2026-01-01 00:00:00') + number, number
    FROM numbers(1000)"

for offset in {0..19}; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO ${TABLE_NAME}
        SELECT toDateTime('2026-01-10 00:00:00') + number * 20 + ${offset}, number
        FROM numbers(1000)"
done

# A second partition is required to activate lazy partition reading.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO ${TABLE_NAME}
    SELECT toDateTime('2026-02-01 00:00:00') + number, number
    FROM numbers(1000)"

QUERY_SETTINGS="
    optimize_read_in_order = 1,
    read_in_order_allow_per_partition_lazy_read = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1,
    read_in_order_two_level_merge_threshold = 10000,
    merge_tree_min_read_task_size = 1024,
    use_query_condition_cache = 0,
    max_block_size = 64,
    max_threads = 4"

echo "lazy path"
if $CLICKHOUSE_CLIENT -q "
    EXPLAIN PIPELINE
    SELECT ts FROM ${TABLE_NAME}
    ORDER BY toUnixTimestamp(ts) LIMIT 20
    SETTINGS ${QUERY_SETTINGS}" | grep -q "Concat"; then
    echo "yes"
else
    echo "no"
fi

echo "partition-local merge"
if $CLICKHOUSE_CLIENT -q "
    EXPLAIN PIPELINE
    SELECT ts FROM ${TABLE_NAME}
    ORDER BY toUnixTimestamp(ts) LIMIT 20
    SETTINGS ${QUERY_SETTINGS}" | grep -q "MergingSortedTransform"; then
    echo "yes"
else
    echo "no"
fi

$CLICKHOUSE_CLIENT -q "
    SELECT ts FROM ${TABLE_NAME}
    ORDER BY toUnixTimestamp(ts) LIMIT 20
    FORMAT Null
    SETTINGS ${QUERY_SETTINGS}, log_comment = '${LOG_COMMENT}'"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS system.query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT if(read_rows <= 512, 'Ok', format('Too many rows read: {}', read_rows))
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND log_comment = '${LOG_COMMENT}' AND type = 'QueryFinish' AND query_kind = 'Select'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE_NAME}"
