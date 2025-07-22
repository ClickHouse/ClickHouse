#!/usr/bin/env bash
# Tags: no-ordinary-database, long, no-flaky-check, no-parallel
#
# - no-flaky-check -- not compatible with ThreadFuzzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT remove_merge_tree_part_delay"

disable_failpoint() {
  $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT remove_merge_tree_part_delay"
}

# Ensure that failpoint is disabled on exit
trap disable_failpoint EXIT

for table in "${TABLES[@]}"; do
  # Should raise either ABORTED or UNKNOWN_TABLE exception
  { $CLICKHOUSE_CLIENT -m << SQL 2>&1
OPTIMIZE TABLE ${table} FINAL;
SQL
} | grep -qP ': Code: (60|236). DB::Exception:' \
    && echo "Table ${table} was not optimized" \
    || echo "Table ${table} raise neither ABORTED nor UNKNOWN_TABLE" &
done | sort -n &

# Disable failpoint in background to execute close to DETACH
disable_failpoint &

# Detach sometimes fails, repeat until it succeeds
# re-attaching must be done after detach to ensure that the database present for the following queries
# Otherwise, it causes `DB::Exception: Database default does not exist. (UNKNOWN_DATABASE)`
while ! $CLICKHOUSE_CLIENT -m 2>/dev/null << SQL
  DETACH DATABASE $CLICKHOUSE_DATABASE;
  ATTACH DATABASE $CLICKHOUSE_DATABASE;
SQL
do
  sleep 0.05
done

$CLICKHOUSE_CLIENT -m << SQL 2>&1
  SYSTEM FLUSH LOGS system.part_log;
  WITH (
      SELECT
        -- Use quantiles to avoid outliers
        quantile(0.25)(duration_ms) AS q25,
        quantile(0.75)(duration_ms) AS q75,
        arrayStringConcat(arraySort(groupArray(duration_ms)), ',') AS all
      FROM system.part_log
      WHERE database = '$CLICKHOUSE_DATABASE' AND error != 0
    ) AS vals
  SELECT if(
    -- Without outliers, 3 is a good threshold
    ((vals.q75 / vals.q25) AS rel) < 3,
    'Merges cancelled quickly',
    printf(
      'Probably merges were cancelled sequentially with global lock held, values: [%s], q25: %.2f, q75: %.2f, ratio: %.2f',
      vals.all,
      vals.q25,
      vals.q75,
      rel
    )
  );
SQL
