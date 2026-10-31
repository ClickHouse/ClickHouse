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

# Case 11: a query eligible for the async queue route (single small block) must not construct or
# start the destination sink before the divert decision is made. MergeTreeSink::onStart() sleeps
# when the destination already has too many parts (`parts_to_delay_insert`); an eagerly-started
# sink would pay that sleep even though the block never reaches it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_no_eager_sink"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_no_eager_sink (id UInt32)
    ENGINE = MergeTree ORDER BY id
    SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 100000,
             min_delay_to_insert_ms = 10000, max_delay_to_insert = 10
"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES test_async_sel_no_eager_sink"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_async_sel_no_eager_sink VALUES (1)"

start_ns=$(date +%s%N)
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=0 -q "
    INSERT INTO test_async_sel_no_eager_sink SELECT 2
"
end_ns=$(date +%s%N)
elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
# A synchronously-started MergeTreeSink would sleep for at least min_delay_to_insert_ms (10000 ms)
# here; the diverted query must return almost immediately instead.
if [ "$elapsed_ms" -lt 6000 ]; then
    echo "ok"
else
    echo "destination sink started eagerly (${elapsed_ms} ms)"
fi

${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES test_async_sel_no_eager_sink"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_no_eager_sink"
