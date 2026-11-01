#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_XXXXXX.sqlite")
NUMERIC_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_numeric_XXXXXX.sqlite")
COMPLEX_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_complex_XXXXXX.sqlite")
NESTED_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_nested_XXXXXX.sqlite")
SPECIAL_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_special_XXXXXX.sqlite")
DYNAMIC_VARIANT_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_dynamic_variant_XXXXXX.sqlite")
MULTIBLOCK_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_multiblock_XXXXXX.sqlite")
EMPTY_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_more_types_empty_XXXXXX.sqlite")
trap 'rm -f "$DB" "$NUMERIC_DB" "$COMPLEX_DB" "$NESTED_DB" "$SPECIAL_DB" "$DYNAMIC_VARIANT_DB" "$MULTIBLOCK_DB" "$EMPTY_DB"' EXIT

STRUCTURE="c1 Enum8('a' = 1), c2 Enum16('b' = 1), c3 Date32, c4 Int128, c5 UInt128, c6 Int256, c7 UInt256, c8 Decimal32(2), c9 Decimal64(2), c10 Decimal128(2), c11 Decimal256(2), c12 UUID, c13 IPv4, c14 IPv6, c15 Bool, c16 Nullable(UInt256), c17 LowCardinality(String), c18 DateTime64(3, 'UTC'), c19 FixedString(4), c20 Date, c21 DateTime('UTC'), c22 LowCardinality(Nullable(String))"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        CAST('a', 'Enum8(\'a\' = 1)') AS c1,
        CAST('b', 'Enum16(\'b\' = 1)') AS c2,
        CAST('2020-01-01', 'Date32') AS c3,
        CAST('9223372036854775808', 'Int128') AS c4,
        CAST(42, 'UInt128') AS c5,
        CAST('-9223372036854775809', 'Int256') AS c6,
        CAST(42, 'UInt256') AS c7,
        CAST(42.42, 'Decimal32(2)') AS c8,
        CAST(42.42, 'Decimal64(2)') AS c9,
        CAST(42.42, 'Decimal128(2)') AS c10,
        CAST(42.42, 'Decimal256(2)') AS c11,
        toUUID('00112233-4455-6677-8899-aabbccddeeff') AS c12,
        toIPv4('192.168.1.10') AS c13,
        toIPv6('2001:db8::1') AS c14,
        CAST(1, 'Bool') AS c15,
        CAST(NULL, 'Nullable(UInt256)') AS c16,
        CAST('low', 'LowCardinality(String)') AS c17,
        toDateTime64('2024-02-29 12:34:56.789', 3, 'UTC') AS c18,
        CAST('test', 'FixedString(4)') AS c19,
        toDate('2024-02-29') AS c20,
        toDateTime('2024-02-29 12:34:56', 'UTC') AS c21,
        CAST('nullable low', 'LowCardinality(Nullable(String))') AS c22
    FORMAT SQLite" > "$DB"

echo "More data types schema inference"
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --output-format TSV \
    --query "DESCRIBE TABLE table" < "$DB" | cut -f1,2

echo "More data types roundtrip"
${CLICKHOUSE_LOCAL} \
    --structure "$STRUCTURE" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT * FROM table" < "$DB"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        CAST(-8, 'Int8') AS i8,
        CAST(-1600, 'Int16') AS i16,
        CAST(-32000, 'Int32') AS i32,
        CAST(-9223372036854775807, 'Int64') AS i64,
        CAST(255, 'UInt8') AS u8,
        CAST(65535, 'UInt16') AS u16,
        CAST(4294967295, 'UInt32') AS u32,
        CAST(1.5, 'Float32') AS f32,
        CAST(-2.25, 'Float64') AS f64,
        CAST(1, 'Bool') AS b,
        toUInt64(9223372036854775808) AS u64,
        CAST('9223372036854775808', 'Int128') AS i128,
        CAST('340282366920938463463374607431768211455', 'UInt128') AS u128,
        CAST('-170141183460469231731687303715884105729', 'Int256') AS i256,
        CAST('340282366920938463463374607431768211456', 'UInt256') AS u256,
        toDecimal64(12.34, 2) AS d
    FORMAT SQLite" > "$NUMERIC_DB"

echo "Native numeric bindings with schema inference"
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --output-format TSV \
    --query "
        SELECT
            i8, toTypeName(i8),
            i16, toTypeName(i16),
            i32, toTypeName(i32),
            i64, toTypeName(i64),
            u8, toTypeName(u8),
            u16, toTypeName(u16),
            u32, toTypeName(u32),
            f32, toTypeName(f32),
            f64, toTypeName(f64),
            b, toTypeName(b),
            u64, toTypeName(u64),
            i128, toTypeName(i128),
            u128, toTypeName(u128),
            i256, toTypeName(i256),
            u256, toTypeName(u256),
            d, toTypeName(d)
        FROM table" < "$NUMERIC_DB"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        tuple(42, 'Hello') AS c1,
        tuple(
            map(42, [1, 2, 3]),
            [
                tuple([tuple(1, 2), tuple(1, 2)], 'Hello', [1, 2, 3]),
                tuple([], 'World', [1])
            ]) AS c2,
        CAST([1, NULL, 3], 'Array(Nullable(UInt16))') AS c3,
        CAST(map('x', toNullable(toUInt32(10)), 'y', CAST(NULL, 'Nullable(UInt32)')), 'Map(String, Nullable(UInt32))') AS c4
    FORMAT SQLite" > "$COMPLEX_DB"

echo "Complex data types roundtrip"
${CLICKHOUSE_LOCAL} \
    --structure "c1 Tuple(UInt32, String), c2 Tuple(Map(UInt32, Array(UInt32)), Array(Tuple(Array(Tuple(UInt32, UInt32)), String, Array(UInt32)))), c3 Array(Nullable(UInt16)), c4 Map(String, Nullable(UInt32))" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT * FROM table" < "$COMPLEX_DB"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        CAST(
            [tuple(
                1,
                CAST('a', 'Nullable(String)'),
                map('k', [toNullable(toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC')), CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))')]))],
            'Array(Tuple(id UInt32, name Nullable(String), events Map(String, Array(Nullable(DateTime64(3, \'UTC\'))))))') AS c1,
        CAST(
            map('group', [tuple(
                toNullable(toUInt8(7)),
                toDecimal64(12.34, 2),
                [tuple('x', toNullable(toUInt16(9))), tuple('y', CAST(NULL, 'Nullable(UInt16)'))])]),
            'Map(String, Array(Tuple(flag Nullable(UInt8), amount Decimal64(2), items Array(Tuple(label String, value Nullable(UInt16))))))') AS c2
    FORMAT SQLite" > "$NESTED_DB"

echo "Nested data types roundtrip"
${CLICKHOUSE_LOCAL} \
    --structure "c1 Array(Tuple(id UInt32, name Nullable(String), events Map(String, Array(Nullable(DateTime64(3, 'UTC')))))), c2 Map(String, Array(Tuple(flag Nullable(UInt8), amount Decimal64(2), items Array(Tuple(label String, value Nullable(UInt16))))))" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT * FROM table" < "$NESTED_DB"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        unhex('275C090A00') AS s,
        CAST(unhex('610062'), 'FixedString(3)') AS fs,
        map('Hello', toDateTime('2020-01-01 00:00:00', 'UTC')) AS m
    FORMAT SQLite" > "$SPECIAL_DB"

echo "String bytes and complex DateTime roundtrip"
${CLICKHOUSE_LOCAL} \
    --structure "s String, fs FixedString(3), m Map(String, DateTime('UTC'))" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT hex(s), length(s), hex(fs), length(fs), m FROM table" < "$SPECIAL_DB"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        1 AS id,
        CAST(NULL, 'Dynamic') AS d,
        CAST(NULL, 'Variant(String, UInt64)') AS v
    UNION ALL
    SELECT
        2,
        CAST('text', 'Dynamic'),
        CAST('text', 'Variant(String, UInt64)')
    FORMAT SQLite" > "$DYNAMIC_VARIANT_DB"

echo "Dynamic and Variant NULL roundtrip"
${CLICKHOUSE_LOCAL} \
    --structure "id UInt8, d Dynamic, v Variant(String, UInt64)" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT * FROM table ORDER BY id" < "$DYNAMIC_VARIANT_DB"

${CLICKHOUSE_LOCAL} --max_block_size 2 --query "
    SELECT
        number AS n,
        [number, number + 1] AS arr,
        map('k' || toString(number), number) AS m
    FROM numbers(5)
    FORMAT SQLite" > "$MULTIBLOCK_DB"

echo "Multi-block roundtrip"
${CLICKHOUSE_LOCAL} \
    --structure "n UInt64, arr Array(UInt64), m Map(String, UInt64)" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT * FROM table ORDER BY n" < "$MULTIBLOCK_DB"

${CLICKHOUSE_LOCAL} --query "SELECT CAST(1, 'UInt8') AS n, 'empty' AS s WHERE 0 FORMAT SQLite" > "$EMPTY_DB"

echo "Empty result set"
${CLICKHOUSE_LOCAL} \
    --structure "n UInt8, s String" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT count() FROM table" < "$EMPTY_DB"
