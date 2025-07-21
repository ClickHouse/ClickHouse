#!/usr/bin/env bash
# Tags: no-ordinary-database, long, no-flaky-check, no-parallel
#
# - no-flaky-check -- not compatible with ThreadFuzzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# to make shellcheck happy about splitting command
function reattach () {
  while ! $CLICKHOUSE_CLIENT -m -q "DETACH DATABASE $CLICKHOUSE_DATABASE; ATTACH DATABASE $CLICKHOUSE_DATABASE;" 2>/dev/null; do
    sleep 0.05
  done
}

export -f reattach

TABLES=( test_shutdown_lock_{01..20} )

for table in "${TABLES[@]}"; do
  # Create tables with as system.metric_log without wide parts,
  # the settings are applied after inserts to avoid excessive memory usage
  $CLICKHOUSE_CLIENT -m << SQL
    SYSTEM FLUSH LOGS system.metric_log;

    CREATE TABLE ${table} AS system.metric_log
    ENGINE = MergeTree
    PARTITION BY ()
    ORDER BY ()
    SETTINGS
        -- prevent merges from being executed automatically
        min_parts_to_merge_at_once = 10
    ;
SQL
done

for table in "${TABLES[@]}"; do
  $CLICKHOUSE_CLIENT -m << SQL &
    INSERT INTO ${CLICKHOUSE_DATABASE}.${table} SELECT * from generateRandom() LIMIT 10;
    INSERT INTO ${CLICKHOUSE_DATABASE}.${table} SELECT * from generateRandom() LIMIT 10;
    INSERT INTO ${CLICKHOUSE_DATABASE}.${table} SELECT * from generateRandom() LIMIT 10;
    INSERT INTO ${CLICKHOUSE_DATABASE}.${table} SELECT * from generateRandom() LIMIT 10;
    INSERT INTO ${CLICKHOUSE_DATABASE}.${table} SELECT * from generateRandom() LIMIT 10;
SQL
done

# Wait for all tables to be created and populated
wait

for table in "${TABLES[@]}"; do
  $CLICKHOUSE_CLIENT -m << SQL
    ALTER TABLE ${CLICKHOUSE_DATABASE}.${table} MODIFY SETTING
        -- horizontal merges does opens all stream at once, so will still use huge amount of memory
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        vertical_merge_algorithm_min_rows_to_activate = 0,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        min_bytes_for_full_part_storage = 0,
        --- avoid excessive memory usage (due to default buffer size of 1MiB that is created for each column)
        max_merge_delayed_streams_for_parallel_write = 1
    ;
SQL
done

# $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT remove_merge_tree_part_delay"

for table in "${TABLES[@]}"; do
  # Should raise either ABORTED or UNKNOWN_TABLE exception
  { $CLICKHOUSE_CLIENT -m << SQL 2>&1
OPTIMIZE TABLE ${table} FINAL;
SQL
} | grep -qP ': Code: (60|236). DB::Exception:' \
    && echo "Table ${table} was not optimized" \
    || echo "Table ${table} raise neither ABORTED nor UNKNOWN_TABLE" &
done | sort -n &

reattach

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT remove_merge_tree_part_delay"

$CLICKHOUSE_CLIENT -m << SQL 2>&1
    SYSTEM FLUSH LOGS system.part_log;
    SELECT if(
        (max(duration_ms) / min(duration_ms)) < 3,
        'Merges cancelled quickly',
        printf(
            'Probably merges were cancelled sequentially with global lock held, min: %i, max: %i, ratio: %.2f',
            min(duration_ms),
            max(duration_ms),
            max(duration_ms) / min(duration_ms)
        )
    ) FROM system.part_log WHERE database = '$CLICKHOUSE_DATABASE' AND error != 0;
SQL
