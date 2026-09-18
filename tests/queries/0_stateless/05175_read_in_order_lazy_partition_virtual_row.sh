#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_lazy_partition_vrow"
DICT_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_lazy_partition_vrow_dict"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE_NAME}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DICT_NAME}"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE_NAME} (ts DateTime, code UInt64)
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(ts)
    ORDER BY (ts, code)
    SETTINGS index_granularity = 64"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${DICT_NAME} (code UInt64, label String)
    ENGINE = MergeTree
    ORDER BY code"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES ${TABLE_NAME}"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO ${DICT_NAME}
    SELECT number * 997, concat('label_', toString(number))
    FROM numbers(23)"

# Multiple interleaved parts make the partition-local merge consume virtual rows
# from several streams. The selective `JOIN` leaves fewer rows than `LIMIT`, so
# the query reads through subsequent blocks and both partitions.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO ${TABLE_NAME}
    SELECT toDateTime('2024-01-01 00:00:00') + number, number
    FROM numbers(1000)"

for offset in {0..19}; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO ${TABLE_NAME}
        SELECT
            toDateTime('2024-01-10 00:00:00') + number * 20 + ${offset},
            1000 + number * 20 + ${offset}
        FROM numbers(1000)"
done

# A second partition is required to activate lazy partition reading.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO ${TABLE_NAME}
    SELECT toDateTime('2024-02-01 00:00:00') + number, 21000 + number
    FROM numbers(1000)"

COMMON_SETTINGS="
    query_plan_read_in_order_through_join = 1,
    query_plan_optimize_join_order_limit = 1,
    query_plan_optimize_join_order_randomize = 0,
    query_plan_join_swap_table = 0,
    max_bytes_ratio_before_external_join = 0,
    max_bytes_before_external_join = 0,
    join_runtime_filter_min_probe_rows = 0,
    enable_join_runtime_filters = 0,
    read_in_order_two_level_merge_threshold = 10000,
    merge_tree_min_read_task_size = 1024,
    use_query_condition_cache = 0,
    max_block_size = 64,
    max_threads = 4"

VIRTUAL_ROW_SETTINGS="
    ${COMMON_SETTINGS},
    optimize_read_in_order = 1,
    read_in_order_allow_per_partition_lazy_read = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1"

QUERY="
    SELECT f.ts, f.code, d.label
    FROM ${TABLE_NAME} AS f
    INNER JOIN ${DICT_NAME} AS d ON f.code = d.code
    ORDER BY toUnixTimestamp(f.ts), f.code, d.label
    LIMIT 100"

PIPELINE=$($CLICKHOUSE_CLIENT -q "
    EXPLAIN PIPELINE ${QUERY}
    SETTINGS ${VIRTUAL_ROW_SETTINGS}")

echo "lazy path active"
if [[ "$PIPELINE" == *"Concat"* && "$PIPELINE" == *"MergingSortedTransform"* ]]; then
    echo "yes"
else
    echo "no"
fi

echo "virtual row conversion active"
if $CLICKHOUSE_CLIENT -q "
    EXPLAIN PLAN actions = 1, indexes = 0 ${QUERY}
    SETTINGS ${VIRTUAL_ROW_SETTINGS}" | grep -q "Virtual row conversions"; then
    echo "yes"
else
    echo "no"
fi

LAZY_RESULT=$($CLICKHOUSE_CLIENT -q "
    ${QUERY}
    SETTINGS ${VIRTUAL_ROW_SETTINGS}")

REFERENCE_RESULT=$($CLICKHOUSE_CLIENT -q "
    ${QUERY}
    SETTINGS ${COMMON_SETTINGS}, optimize_read_in_order = 0")

echo "same result"
if [[ "$LAZY_RESULT" == "$REFERENCE_RESULT" ]]; then
    echo "yes"
else
    echo "no"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE_NAME}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DICT_NAME}"
