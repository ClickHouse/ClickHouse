#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regressions of INSERT ... SELECT under async_insert that need no concurrent DDL. Cases with an
# insert held in flight while DDL lands live in 04633_async_insert_select_alter_race and
# 04633_async_insert_select_freeze_race; all three used to be one 3-minute test. Case numbers kept.

# Case 1: an empty INSERT ... SELECT must still run the insert pipeline so side-effecting
# destinations (file table functions) get created even with zero SELECT rows. If the file
# was never created, the FROM INFILE read below fails.
FILE_EMPTY="${CLICKHOUSE_USER_FILES_UNIQUE:?}_04633_empty.csv"
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

# Case 2: multi-block INSERT ... SELECT with a Nullable/expression column into a Nullable column
# must not crash with a schema-conversion logical error. max_block_size=1 forces the fallback path.
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

# Case 7 and 8: the synchronous fallback must apply the same dedup decision as a plain sync
# insert. max_block_size=1 forces the multi-block sync fallback for a 10-row SELECT.
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
