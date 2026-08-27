#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# Nested ClickHouse types (Array, Map, Tuple/struct, Nullable, and their combinations) are supported for
# CREATE TABLE and mapped to the corresponding Delta complex types (array, map, struct).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_nested_types"
INITIAL_LOG="${TABLE_PATH}/_delta_log/00000000000000000000.json"

rm -rf "$TABLE_PATH"

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
DROP TABLE IF EXISTS t_dl_nested;
CREATE TABLE t_dl_nested (
    c_arr       Array(Int32),
    c_map       Map(String, Int64),
    c_tuple     Tuple(a Int32, b String),
    c_nested    Array(Tuple(x Int32, y Array(String))),
    c_nullable  Nullable(Int32)
) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"

if [ -f "$INITIAL_LOG" ]; then
    echo "post-create: initial commit exists"
else
    echo "post-create: fail: initial commit was not written"
    exit 1
fi

# The Delta complex types must be what is persisted in commit 0 (the schema lives in the JSON-encoded
# `metaData.schemaString`, so type names are quoted and backslash-escaped, e.g. \"array\").
for delta_type in array map struct; do
    if ! grep -qF "\\\"$delta_type\\\"" "$INITIAL_LOG"; then
        echo "fail: Delta type $delta_type not found in initial commit"
        exit 1
    fi
done
echo "commit-json: contains nested Delta types"

# The table must be readable (empty), proving the nested-type mapping does not break read-back.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
SELECT count() FROM t_dl_nested;
DROP TABLE t_dl_nested;
"

rm -rf "$TABLE_PATH"
