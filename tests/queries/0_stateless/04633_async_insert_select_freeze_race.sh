#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT ... SELECT under async_insert resolves the destination schema before the SELECT
# starts; a DDL query arriving mid-flight must not change it. Split out of
# 04633_async_insert_select_regression. Case numbers kept from that test.

# Case 9: concurrent ADD COLUMN must not corrupt a column-transformer INSERT (async path).
# `* EXCEPT c` must resolve against the metadata frozen before the SELECT runs.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_transformer_single"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_transformer_single (a UInt32, b UInt32, c UInt32 DEFAULT 77)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --optimize_trivial_insert_select=1 --async_insert=1 --wait_for_async_insert=1 --query_id insert_case9_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_transformer_single (* EXCEPT c)
    SELECT number AS a, number * 2 AS b
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case9_${CLICKHOUSE_DATABASE}" 30
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_transformer_single ADD COLUMN d UInt32 DEFAULT 99"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_transformer_single"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_transformer_single WHERE c = 77"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_transformer_single WHERE d = 99"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_transformer_single"

# Case 10: same column-transformer race on the sync-fallback path (schema freeze, multi-block).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_transformer_multi"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_transformer_multi (a UInt32, b UInt32, c UInt32 DEFAULT 77)
    ENGINE = MergeTree ORDER BY a
"
${CLICKHOUSE_CLIENT} \
    --max_block_size=1000 --async_insert=1 --wait_for_async_insert=1 --query_id insert_case10_${CLICKHOUSE_DATABASE} -q "
    INSERT INTO test_async_sel_transformer_multi (* EXCEPT c)
    SELECT number AS a, number * 2 AS b
    FROM numbers(2000)
    WHERE sleepEachRow(0.001) = 0
" &
INSERT_PID=$!
wait_for_query_to_start "insert_case10_${CLICKHOUSE_DATABASE}" 30
${CLICKHOUSE_CLIENT} -q "ALTER TABLE test_async_sel_transformer_multi ADD COLUMN d UInt32 DEFAULT 99"
wait "$INSERT_PID"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_transformer_multi"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_transformer_multi WHERE c = 77"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_async_sel_transformer_multi WHERE d = 99"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_transformer_multi"
