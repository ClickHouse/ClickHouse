#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# Issue #103155: CREATE TABLE on a fresh location (no preexisting `_delta_log`)
# must drive the delta-kernel-rs create-table transaction and persist a commit
# whose Metadata action carries all column names, types and nullability.
# This exercises a wider type matrix than 04260_create_table_deltalake_writes_initial_log.sh
# so we catch regressions in the C++ -> kernel schema visitor.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_schema_types"
INITIAL_LOG="${TABLE_PATH}/_delta_log/00000000000000000000.json"

rm -rf "$TABLE_PATH"
[ -d "${TABLE_PATH}/_delta_log" ] && echo "fail: _delta_log unexpectedly present before CREATE TABLE" && exit 1
echo "pre-create: no _delta_log"

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

DROP TABLE IF EXISTS t_dl_schema_types;
CREATE TABLE t_dl_schema_types (
    c_byte    Int8,
    c_short   Int16,
    c_int     Int32,
    c_long    Int64,
    c_float   Float32,
    c_double  Float64,
    c_string  String,
    c_n_int   Nullable(Int32),
    c_n_str   Nullable(String)
) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"

# The kernel create-table transaction must have written commit version 0.
if [ ! -f "$INITIAL_LOG" ]; then
    echo "fail: initial commit was not written at $INITIAL_LOG"
    exit 1
fi
echo "post-create: initial commit exists"

# Verify the kernel can read the table back: empty, with the declared columns.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

SELECT count() FROM t_dl_schema_types;
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_dl_schema_types'
ORDER BY name;

DROP TABLE t_dl_schema_types;
"

# The commit JSON must contain every logical column name so downstream readers
# (kernel or another Delta implementation) reconstruct the schema correctly.
for col in c_byte c_short c_int c_long c_float c_double c_string c_n_int c_n_str; do
    if ! grep -q "\"$col\"" "$INITIAL_LOG"; then
        echo "fail: column $col not found in initial commit"
        exit 1
    fi
done
echo "commit-json: contains all declared column names"

rm -rf "$TABLE_PATH"
