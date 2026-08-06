#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# ClickHouse types that are not identical to a Delta type but can be stored without loss are accepted for
# CREATE TABLE and mapped to a compatible (wider/looser) Delta type. ClickHouse does the query processing,
# so reading such a column back may yield a slightly different ClickHouse type (e.g. `UInt8` -> `Int16`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_compatible_types"
INITIAL_LOG="${TABLE_PATH}/_delta_log/00000000000000000000.json"

rm -rf "$TABLE_PATH"

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_experimental_delta_lake_create_table = 1;
DROP TABLE IF EXISTS t_dl_compat;
CREATE TABLE t_dl_compat (
    c_u8      UInt8,
    c_u16     UInt16,
    c_u32     UInt32,
    c_fixed   FixedString(4),
    c_date    Date,
    c_dt      DateTime,
    c_dt64_3  DateTime64(3),
    c_dt64_6  DateTime64(6)
) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"

if [ -f "$INITIAL_LOG" ]; then
    echo "post-create: initial commit exists"
else
    echo "post-create: fail: initial commit was not written"
    exit 1
fi

# The compatible Delta types must be what is persisted in commit 0 (the schema lives in the JSON-encoded
# `metaData.schemaString`, so type names are quoted and backslash-escaped, e.g. \"short\").
for delta_type in short integer long string date timestamp; do
    if ! grep -qF "\\\"$delta_type\\\"" "$INITIAL_LOG"; then
        echo "fail: Delta type $delta_type not found in initial commit"
        exit 1
    fi
done
echo "commit-json: contains mapped Delta types"

# The table must be readable (empty), proving the compatible-type mapping does not break read-back.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_experimental_delta_lake_create_table = 1;
SELECT count() FROM t_dl_compat;
DROP TABLE t_dl_compat;
"

rm -rf "$TABLE_PATH"
