#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ColumnBinary/COLUMNAR_V1 cannot represent Map, nested Nullable/Variant inside
# Array/Tuple, or fixed-width types wider than 8 bytes (UUID, IPv6, Int128/UInt128,
# Decimal128/256). These signatures must be rejected at format construction time,
# before any block is serialized, rather than failing partway through the first block.

${CLICKHOUSE_CLIENT} --query "SELECT map('a', 1) AS m FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "Map is not supported"

${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4() AS u FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "fixed-width type wider than 8 bytes"

${CLICKHOUSE_CLIENT} --query "SELECT toDecimal128(1.5, 2) AS d FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "fixed-width type wider than 8 bytes"

${CLICKHOUSE_CLIENT} --query "SELECT [NULL::Nullable(String)] AS a FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "nested Nullable inside Array/Tuple"

${CLICKHOUSE_CLIENT} --query "SELECT tuple(NULL::Nullable(UInt64)) AS t FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "nested Nullable inside Array/Tuple"

# A well-formed signature is unaffected.
${CLICKHOUSE_CLIENT} --query "SELECT 'ok' AS s FROM numbers(1) FORMAT ColumnBinary" | wc -c
