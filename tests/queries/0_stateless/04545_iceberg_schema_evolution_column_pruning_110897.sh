#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - no-parallel: asserts on per-query ProfileEvents read from system.query_log,
#   so it must not race other queries flushing into the shared query_log; it
#   also compares decoded byte counts, which parallel cache/IO pressure could
#   perturb. Unique table/database names isolate data but not those globals.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/110897 :
# after an Iceberg table's schema evolves (e.g. ADD COLUMN), reads of data files
# written under the old schema stopped pruning Parquet columns, so a query that
# needs only `id` decoded every physical column of the old file (including wide
# string columns it never referenced). This asserts pruning is restored while
# results stay correct.

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null

# id + one wide string column. min_bytes_for_wide_part is irrelevant (Iceberg
# stores Parquet); the wide `s1` is what makes full-schema reads expensive.
# max_insert_threads=1: a parallel insert writes one data file per thread and
# commits each as a SEPARATE Iceberg snapshot, so a read can resolve an
# intermediate snapshot and see a partial row set. One thread = one atomic commit.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64, s1 String, k Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --max_insert_threads=1 --query \
    "INSERT INTO ${TABLE} SELECT number, repeat('x', 200), number % 100 FROM numbers(200000)"

# Schema evolution: add an unrelated nullable column. The already-written data
# file now has a schema id different from the table's current schema id, which
# is the trigger for the pruning-disabled path.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} ADD COLUMN s2 Nullable(String)"

# --- Correctness: results must be identical to a non-evolved read ------------
# sum over id only (the reported query).
${CLICKHOUSE_CLIENT} --query "SELECT 'sum_id', sum(id) FROM ${TABLE}"
# The added column reads as all-NULL for the old file.
${CLICKHOUSE_CLIENT} --query "SELECT 'added_col_null', count(), countIf(s2 IS NULL) FROM ${TABLE}"
# The wide column is still fully readable when actually requested.
${CLICKHOUSE_CLIENT} --query "SELECT 'wide_col', any(length(s1)), sum(length(s1)) FROM ${TABLE}"
# Filter on a column the projection does not output (k) must still work.
${CLICKHOUSE_CLIENT} --query "SELECT 'filter_nonsel', sum(id) FROM ${TABLE} WHERE k = 7"
# PREWHERE on the wide column.
${CLICKHOUSE_CLIENT} --query "SELECT 'prewhere', count() FROM ${TABLE} PREWHERE s1 = repeat('x', 200)"

# --- Pruning: the fix must not read the wide column for an id-only query ------
# Robust, randomization-proof assertion: an id-only read must decode far fewer
# bytes than a read that genuinely needs the wide `s1` column. Before the fix
# both decoded the full old schema, so the ratio was ~1; after the fix the
# id-only read is an order of magnitude smaller. We require a >=3x gap, well
# below the observed ~23x but safely above any per-run noise.
Q_ID="qid_${CLICKHOUSE_DATABASE}_${RANDOM}"
Q_S1="qs1_${CLICKHOUSE_DATABASE}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query "SELECT sum(id) FROM ${TABLE} SETTINGS log_comment='${Q_ID}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT sum(length(s1)) FROM ${TABLE} SETTINGS log_comment='${Q_S1}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
SELECT 'id_read_is_pruned',
       (SELECT ProfileEvents['SelectedBytes'] FROM system.query_log
          WHERE log_comment = '${Q_ID}' AND type = 'QueryFinish'
            AND current_database = currentDatabase()
          ORDER BY event_time_microseconds DESC LIMIT 1) * 3
       <
       (SELECT ProfileEvents['SelectedBytes'] FROM system.query_log
          WHERE log_comment = '${Q_S1}' AND type = 'QueryFinish'
            AND current_database = currentDatabase()
          ORDER BY event_time_microseconds DESC LIMIT 1)
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}" 2>/dev/null

# --- Other schema-evolution shapes: rename, removal, type change -------------
# The reported issue used ADD COLUMN; these apply the other evolutions the
# transform DAG can produce (rename -> alias, drop -> pruned input, type change
# -> cast) to the same old-schema data file, chained so it also covers multiple
# evolutions. Each must keep results correct AND still prune the wide column.
T2="t2_${CLICKHOUSE_DATABASE}_${RANDOM}"
T2_PATH="${USER_FILES_PATH}/${T2}/"
rm -rf "${T2_PATH}" 2>/dev/null

# id + wide s1 + a to-be-dropped wide column d + an int k to be retyped.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T2} (id Int64, s1 String, d String, k Int32) ENGINE = IcebergLocal('${T2_PATH}', 'Parquet')"
# max_insert_threads=1 for a single atomic snapshot (see note on the first insert).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --max_insert_threads=1 --query \
    "INSERT INTO ${T2} SELECT number, repeat('x', 200), repeat('y', 150), toInt32(number % 100) FROM numbers(200000)"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${T2} RENAME COLUMN s1 TO s1_wide"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${T2} DROP COLUMN d"
# int -> long is one of Iceberg's allowed primitive promotions.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${T2} MODIFY COLUMN k Int64"

# Correctness after all three evolutions on the old data file.
${CLICKHOUSE_CLIENT} --query "SELECT 'evolved_sum_id', sum(id) FROM ${T2}"
${CLICKHOUSE_CLIENT} --query "SELECT 'renamed_col', any(length(s1_wide)), sum(length(s1_wide)) FROM ${T2}"
${CLICKHOUSE_CLIENT} --query "SELECT 'retyped_k', sum(k), any(toTypeName(k)) FROM ${T2}"

# Pruning still holds on the fully-evolved schema: id-only decodes far fewer
# bytes than reading the renamed wide column (same >=3x check as above).
Q2_ID="q2id_${CLICKHOUSE_DATABASE}_${RANDOM}"
Q2_W="q2w_${CLICKHOUSE_DATABASE}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query "SELECT sum(id) FROM ${T2} SETTINGS log_comment='${Q2_ID}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT sum(length(s1_wide)) FROM ${T2} SETTINGS log_comment='${Q2_W}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
SELECT 'evolved_read_is_pruned',
       (SELECT ProfileEvents['SelectedBytes'] FROM system.query_log
          WHERE log_comment = '${Q2_ID}' AND type = 'QueryFinish'
            AND current_database = currentDatabase()
          ORDER BY event_time_microseconds DESC LIMIT 1) * 3
       <
       (SELECT ProfileEvents['SelectedBytes'] FROM system.query_log
          WHERE log_comment = '${Q2_W}' AND type = 'QueryFinish'
            AND current_database = currentDatabase()
          ORDER BY event_time_microseconds DESC LIMIT 1)
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${T2}"
rm -rf "${T2_PATH}" 2>/dev/null
