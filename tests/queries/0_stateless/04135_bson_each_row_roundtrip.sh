#!/usr/bin/env bash

# Round-trip a wide variety of ClickHouse types through BSONEachRow.
# Exercises Processors/Formats/Impl/BSONEachRowRowInputFormat.cpp for each
# BSON element type (int32, int64, double, string, bool, array, document,
# date-time, UUID/binary) plus the output format.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OUT="${CLICKHOUSE_TMP:-/tmp}/${CLICKHOUSE_TEST_UNIQUE_NAME}.bson"

# -----------------------------------------------------------------------------
# 1. Write BSON with the full type soup, then read it back.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT
    1::UInt8 AS u8,
    2::Int8 AS i8,
    3::UInt16 AS u16,
    -4::Int16 AS i16,
    5::UInt32 AS u32,
    -6::Int32 AS i32,
    7::UInt64 AS u64,
    -8::Int64 AS i64,
    9.5::Float32 AS f32,
    10.5::Float64 AS f64,
    true AS b,
    'hi' AS s,
    [1, 2]::Array(Int32) AS arr,
    (1, 'a') AS tup,
    map('k', 'v') AS m,
    toDateTime('2023-01-02 03:04:05', 'UTC') AS dt,
    toDate('2023-01-02') AS d,
    toUUID('00000000-0000-0000-0000-000000000001') AS u
FORMAT BSONEachRow
" > "${OUT}"

echo '--- BSON -> Typed schema ---'
${CLICKHOUSE_LOCAL} --query "
SELECT * FROM file('${OUT}', 'BSONEachRow',
    'u8 UInt8, i8 Int8, u16 UInt16, i16 Int16, u32 UInt32, i32 Int32,
     u64 UInt64, i64 Int64, f32 Float32, f64 Float64, b Bool, s String,
     arr Array(Int32), tup Tuple(Int32, String), m Map(String, String),
     dt DateTime(''UTC''), d Date, u UUID')
FORMAT Vertical
"

# -----------------------------------------------------------------------------
# 2. Cross-type coercion: write as Int32, read as Int64/Int8/String.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT 42::Int32 AS v FORMAT BSONEachRow
" > "${OUT}"

echo '--- coerce Int32 -> Int64 ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 'v Int64')"

echo '--- coerce Int32 -> Int8 ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 'v Int8')"

echo '--- error: coerce Int32 -> Float64 (not allowed) ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 'v Float64')" 2>&1 | grep -o "Code: [0-9]*" | head -1

# -----------------------------------------------------------------------------
# 3. Nullable handling: BSON null -> Nullable column.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT NULL::Nullable(Int32) AS x, 1::Nullable(Int32) AS y FORMAT BSONEachRow
" > "${OUT}"

echo '--- Nullable round-trip ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 'x Nullable(Int32), y Nullable(Int32)')"

# -----------------------------------------------------------------------------
# 4. Multiple rows in one BSON stream.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT number AS n, toString(number) AS s FROM numbers(5) FORMAT BSONEachRow
" > "${OUT}"

echo '--- multiple rows ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 'n UInt64, s String') ORDER BY n"

# -----------------------------------------------------------------------------
# 5. Schema inference (no types provided).
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT 1::Int32 AS a, 'x' AS s, true AS b, 1.5 AS f, [1, 2]::Array(Int32) AS arr FORMAT BSONEachRow
" > "${OUT}"

echo '--- schema inference ---'
${CLICKHOUSE_LOCAL} --query "DESCRIBE TABLE file('${OUT}', 'BSONEachRow')" | sort

# -----------------------------------------------------------------------------
# 6. Nested document round-trip (BSON document -> Tuple named fields).
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT (1::Int32, 'hi') AS t FORMAT BSONEachRow
" > "${OUT}"

echo '--- document -> Tuple ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 't Tuple(a Int32, b String)')"

# -----------------------------------------------------------------------------
# 7. Error: wrong BSON type for target column.
# -----------------------------------------------------------------------------
${CLICKHOUSE_LOCAL} --query "
SELECT 'string' AS v FORMAT BSONEachRow
" > "${OUT}"

echo '--- error: BSON string -> column of Int32 ---'
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OUT}', 'BSONEachRow', 'v Int32')" 2>&1 | grep -o "Cannot\|Code: [0-9]*" | head -1

rm -f "${OUT}"
