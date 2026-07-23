#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${CUR_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

rm -rf "$BASE_DIR" 2>/dev/null || true

# First write to create the file, then TRUNCATE to overwrite it.
# Both should work with into_outfile_create_parent_directories.
$CLICKHOUSE_CLIENT -q "SET into_outfile_create_parent_directories = 1; SELECT 1 INTO OUTFILE '${BASE_DIR}/a/b/c/test.tsv'"
$CLICKHOUSE_CLIENT -q "SET into_outfile_create_parent_directories = 1; SELECT 2 INTO OUTFILE '${BASE_DIR}/a/b/c/test.tsv' TRUNCATE"

cat "${BASE_DIR}/a/b/c/test.tsv"

rm -rf "$BASE_DIR" 2>/dev/null || true
