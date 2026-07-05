#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ColumnBinary/COLUMNAR_V1 cannot represent Map, LowCardinality, nested Nullable/Variant
# inside Array/Tuple, or fixed-width types whose size isn't exactly 1/2/4/8 bytes (UUID,
# IPv6, Int128/UInt128, Decimal128/256, and any-length FixedString all fall outside that
# set). These signatures must be rejected at format construction time, before any block
# is serialized, rather than failing partway through the first block.

${CLICKHOUSE_CLIENT} --query "SELECT map('a', 1) AS m FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "Map is not supported"

${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4() AS u FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "fixed-width type of size"

${CLICKHOUSE_CLIENT} --query "SELECT toDecimal128(1.5, 2) AS d FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "fixed-width type of size"

# FixedString(3) is only 3 bytes -- within the old ">8 bytes" check's blind spot -- but
# buildColDescriptor only has top-level wire tags for widths 1/2/4/8.
${CLICKHOUSE_CLIENT} --query "SELECT toFixedString('abc', 3) AS f FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "fixed-width type of size"

${CLICKHOUSE_CLIENT} --query "SELECT [NULL::Nullable(String)] AS a FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "nested Nullable inside Array/Tuple"

${CLICKHOUSE_CLIENT} --query "SELECT tuple(NULL::Nullable(UInt64)) AS t FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "nested Nullable inside Array/Tuple"

${CLICKHOUSE_CLIENT} --query "SELECT toLowCardinality('a') AS x FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "LowCardinality is not supported"

${CLICKHOUSE_CLIENT} --query "SELECT [toLowCardinality('a')] AS x FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "LowCardinality is not supported"

# Nullable(Tuple(...)) has no COL_COMPLEX null-map slot at the top level either
# (only COL_BYTES/COL_FIXED* carry COL_IS_NULLABLE); without this check it slips past
# validation and crashes deep in buildColDescriptor instead of being rejected upfront.
${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --query "SELECT CAST(tuple(1), 'Nullable(Tuple(UInt64))') AS t FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "Nullable(Array/Tuple/Variant) is not supported"

# A well-formed signature is unaffected.
${CLICKHOUSE_CLIENT} --query "SELECT 'ok' AS s FROM numbers(1) FORMAT ColumnBinary" | wc -c
