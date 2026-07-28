#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Case 1: an empty INSERT ... SELECT must still execute the insert pipeline so that
# side-effecting destinations (file table functions, etc.) are created even when SELECT
# returns zero rows. Mirror the 03277 pattern: write to a CSV file from an empty Join table,
# then read back from the file; if the file was never created this FROM INFILE fails.
FILE_EMPTY="${CLICKHOUSE_USER_FILES_UNIQUE:?}_04649_empty.csv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_empty_src"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_empty_src (id UInt32)
    ENGINE = Join(ANY, INNER, id)
"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO TABLE FUNCTION file('${FILE_EMPTY}', 'CSV', 'id UInt32')
    SELECT id FROM test_async_sel_empty_src
"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_async_sel_empty_src (id)
    FROM INFILE '${FILE_EMPTY}' FORMAT CSV
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_empty_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_empty_src"
rm -f "${FILE_EMPTY}"

# Case 2: multi-block INSERT ... SELECT with a Nullable/expression column into a table with a
# Nullable column must not crash with a schema-conversion logical error.
# max_block_size=1 forces the multi-block fallback path.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_nullable"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_nullable (v Nullable(UInt64))
    ENGINE = MergeTree ORDER BY tuple()
"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_nullable
    SELECT toNullable(number) AS v FROM numbers(5)
    SETTINGS max_block_size = 1
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_nullable"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_nullable"

# Case 3: concurrent MODIFY COLUMN FIRST must not corrupt data (MatchColumnsMode::Name regression).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_alter_race"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_alter_race (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=500000 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_alter_race
    SELECT number AS a, number * 2 AS b
    FROM numbers(1000000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.5
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_alter_race MODIFY COLUMN b UInt32 FIRST"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_alter_race WHERE b != a * 2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_alter_race"

# Case 4: concurrent ADD COLUMN must not cause THERE_IS_NO_COLUMN (schema freeze, async path).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_add_col_single"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_add_col_single (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_add_col_single
    SELECT number AS a, number * 2 AS b
    FROM numbers(200000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.2
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_add_col_single ADD COLUMN c UInt32 DEFAULT 42"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single WHERE b = a * 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_single WHERE c = 42"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_add_col_single"

# Case 5: same ADD COLUMN race on the sync-fallback path (schema freeze, multi-block).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_add_col_multi"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_add_col_multi (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=500000 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_add_col_multi
    SELECT number AS a, number * 2 AS b
    FROM numbers(1000000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.5
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_add_col_multi ADD COLUMN c UInt32 DEFAULT 42"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi WHERE b = a * 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_add_col_multi WHERE c = 42"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_add_col_multi"

# Case 6: concurrent CREATE MATERIALIZED VIEW ... TO <destination> must not receive rows from an
# already-running INSERT ... SELECT that fell back to the synchronous path. The dependency graph
# is frozen before the SELECT starts, so a view created afterward must get zero rows from this
# query even though the destination table itself gets the full result.
# max_block_size forces the multi-block sync fallback.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_mv_race_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_mv_race_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS test_async_sel_mv_race_mv"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_mv_race_dst (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_mv_race_target (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=500000 --async_insert=1 --wait_for_async_insert=1 -q "
    INSERT INTO test_async_sel_mv_race_dst
    SELECT number AS a, number * 2 AS b
    FROM numbers(1000000)
    WHERE sleepEachRow(0.000002) = 0
" &
INSERT_PID=$!
sleep 0.5
${CLICKHOUSE_CLIENT} -q "
    CREATE MATERIALIZED VIEW test_async_sel_mv_race_mv TO test_async_sel_mv_race_target AS
    SELECT * FROM test_async_sel_mv_race_dst
"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_mv_race_dst"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_mv_race_target"
${CLICKHOUSE_CLIENT} -q "DROP VIEW test_async_sel_mv_race_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_mv_race_target"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_mv_race_dst"

# Case 7 and 8: the synchronous fallback must apply the same INSERT ... SELECT deduplication
# decision as a plain synchronous insert, not the decision latched before that override.
# max_block_size=1 forces the multi-block sync fallback for a 10-row SELECT.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_dedup"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_dedup (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS non_replicated_deduplication_window = 1000
"

for _ in 1 2; do
${CLICKHOUSE_CLIENT} \
    --async_insert=1 --wait_for_async_insert=1 --insert_deduplicate=1 --max_block_size=1 -q "
    INSERT INTO test_async_sel_dedup SELECT number AS id, toString(number) AS data FROM numbers(10)
"
done
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_dedup"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_async_sel_dedup"

for _ in 1 2; do
${CLICKHOUSE_CLIENT} \
    --async_insert=1 --wait_for_async_insert=1 --insert_deduplicate=0 \
    --deduplicate_insert_select=force_enable --max_block_size=1 -q "
    INSERT INTO test_async_sel_dedup SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY ALL
"
done
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_dedup"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_dedup"
