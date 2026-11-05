#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_rename_substreams"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_rename_substreams
    (
        id UInt32,
        arr Array(UInt32)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0,
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_rename_substreams SELECT number, [number, number + 1] FROM numbers(10)"

# Get data path before rename to verify initial state
DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'test_rename_substreams' AND active")

echo "Before rename:"
cat "${DATA_PATH}columns_substreams.txt"

# Rename arr -> brr
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test_rename_substreams RENAME COLUMN arr TO brr"

# Get the new part path (mutation creates a new part)
DATA_PATH_NEW=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'test_rename_substreams' AND active")

echo "After rename arr -> brr:"
cat "${DATA_PATH_NEW}columns_substreams.txt"

# Also rename a Nested column to test the second code path (line 450)
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_rename_nested_substreams"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_rename_nested_substreams
    (
        id UInt32,
        nested Nested(a UInt32, b UInt32)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0,
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_rename_nested_substreams SELECT number, [number], [number + 1] FROM numbers(10)"

DATA_PATH_NESTED=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'test_rename_nested_substreams' AND active")

echo "Nested before rename:"
cat "${DATA_PATH_NESTED}columns_substreams.txt"

# Rename nested.a -> nested.aa
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test_rename_nested_substreams RENAME COLUMN nested.a TO nested.aa"

DATA_PATH_NESTED_NEW=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'test_rename_nested_substreams' AND active")

echo "Nested after rename nested.a -> nested.aa:"
cat "${DATA_PATH_NESTED_NEW}columns_substreams.txt"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_rename_substreams"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_rename_nested_substreams"
