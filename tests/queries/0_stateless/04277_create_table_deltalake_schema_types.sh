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
#
# It also checks the type-round-trip contract: only ClickHouse types that read back
# unchanged through Delta metadata are accepted (e.g. `Bool` is committed as Delta
# `boolean`, NOT plain `UInt8`), and non-round-tripping types are rejected up front.

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
    c_bool    Bool,
    c_date    Date32,
    c_ts      DateTime64(6),
    c_decimal Decimal(10, 2),
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

# Verify the kernel can read the table back: empty, with the declared columns, and every type
# identical to what was declared (the round-trip contract; a `Bool` written as Delta `boolean`
# must read back as `Bool`, not `UInt8`).
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
# Column names live in the `metaData.schemaString` field, which is itself a
# JSON-encoded string, so the quotes around each name are backslash-escaped
# (e.g. \"c_byte\"). Match that escaped form with a fixed-string grep.
for col in c_byte c_short c_int c_long c_float c_double c_string c_bool c_date c_ts c_decimal c_n_int c_n_str; do
    if ! grep -qF "\\\"$col\\\"" "$INITIAL_LOG"; then
        echo "fail: column $col not found in initial commit"
        exit 1
    fi
done
echo "commit-json: contains all declared column names"

rm -rf "$TABLE_PATH"

# Types with no loss-free Delta representation must be rejected before commit 0 is written
# (Code: 48 = NOT_IMPLEMENTED): `UInt64` (exceeds Delta's signed 64-bit `long`), `Decimal` with precision
# above 38, and `LowCardinality` (no Delta equivalent). Compatible types (`UInt8`, `FixedString`, `Date`,
# `DateTime`, ...) are instead accepted and mapped to a wider/looser Delta type - see the compatible-types test.
echo "rejections:"
for spec in "UInt64" "Decimal(50, 2)" "LowCardinality(String)" "DateTime('UTC')" "DateTime64(6, 'UTC')"; do
    reject_path="${TABLE_PATH}_reject"
    rm -rf "$reject_path"
    if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_reject (c ${spec}) ENGINE = DeltaLakeLocal('${reject_path}', Parquet);
" 2>&1 | grep -q "Code: 48"; then
        echo "${spec}: rejected"
    else
        echo "${spec}: NOT rejected"
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_reject" >/dev/null 2>&1
    rm -rf "$reject_path"
done

# A column whose name is reserved for a virtual column (e.g. `_path`) must be rejected *before* commit 0
# is written (Code: 44 = ILLEGAL_COLUMN); otherwise a rejected CREATE would leave an orphan Delta table
# behind at the target path.
echo "reserved-virtual-column:"
reserved_path="${TABLE_PATH}_reserved"
rm -rf "$reserved_path"
if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_reserved (_path String, id Int32) ENGINE = DeltaLakeLocal('${reserved_path}', Parquet);
" 2>&1 | grep -q "Code: 44"; then
    echo "_path: rejected"
else
    echo "_path: NOT rejected"
fi
if [ -d "${reserved_path}/_delta_log" ]; then
    echo "_path: fail: orphan _delta_log left behind"
else
    echo "_path: no orphan _delta_log"
fi
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_reserved" >/dev/null 2>&1
rm -rf "$reserved_path"

# A special column (MATERIALIZED / ALIAS / EPHEMERAL) must also be rejected *before* commit 0 is written
# (Code: 36 = BAD_ARGUMENTS); otherwise a rejected CREATE would leave an orphan Delta table behind.
echo "special-column:"
special_path="${TABLE_PATH}_special"
rm -rf "$special_path"
if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_special (id Int32, m Int32 MATERIALIZED 1) ENGINE = DeltaLakeLocal('${special_path}', Parquet);
" 2>&1 | grep -q "Code: 36"; then
    echo "MATERIALIZED: rejected"
else
    echo "MATERIALIZED: NOT rejected"
fi
if [ -d "${special_path}/_delta_log" ]; then
    echo "MATERIALIZED: fail: orphan _delta_log left behind"
else
    echo "MATERIALIZED: no orphan _delta_log"
fi
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_special" >/dev/null 2>&1
rm -rf "$special_path"
