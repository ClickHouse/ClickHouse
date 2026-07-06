#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ColumnBinary/COLUMNAR_V1 cannot represent Map, LowCardinality, or Variant nested inside
# Array/Tuple. These signatures must be rejected at format construction time, before any
# block is serialized, rather than failing partway through the first block. Fixed-width
# types of any size (UUID, IPv6, Int128/UInt128, Decimal128/256) and FixedString(N) of
# any length are supported via COL_FIXEDN — see 04506_column_binary_wide_fixed_width for
# their round-trip coverage. Nullable(T) nested inside Array/Tuple is supported too —
# see 04507_column_binary_nested_nullable for its round-trip coverage.

${CLICKHOUSE_CLIENT} --query "SELECT map('a', 1) AS m FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "Map is not supported"

${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4() AS u FROM numbers(1) FORMAT ColumnBinary" | wc -c

${CLICKHOUSE_CLIENT} --query "SELECT toDecimal128(1.5, 2) AS d FROM numbers(1) FORMAT ColumnBinary" | wc -c

${CLICKHOUSE_CLIENT} --query "SELECT toFixedString('abc', 3) AS f FROM numbers(1) FORMAT ColumnBinary" | wc -c

${CLICKHOUSE_CLIENT} --query "SELECT [NULL::Nullable(String)] AS a FROM numbers(1) FORMAT ColumnBinary" | wc -c

${CLICKHOUSE_CLIENT} --query "SELECT tuple(NULL::Nullable(UInt64)) AS t FROM numbers(1) FORMAT ColumnBinary" | wc -c

${CLICKHOUSE_CLIENT} --query "SELECT toLowCardinality('a') AS x FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "LowCardinality is not supported"

${CLICKHOUSE_CLIENT} --query "SELECT [toLowCardinality('a')] AS x FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "LowCardinality is not supported"

# Nullable(Tuple(...)) is supported (see 04505_column_binary_nullable_tuple for the
# round-trip check); Nullable(Array(...)) stays rejected defensively even though
# DataTypeArray::canBeInsideNullable() already makes it unreachable from SQL.
${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --query "SELECT CAST(tuple(1), 'Nullable(Tuple(UInt64))') AS t FROM numbers(1) FORMAT ColumnBinary" | wc -c

# A well-formed signature is unaffected.
${CLICKHOUSE_CLIENT} --query "SELECT 'ok' AS s FROM numbers(1) FORMAT ColumnBinary" | wc -c
