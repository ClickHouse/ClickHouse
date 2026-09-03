#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# Top-level LowCardinality(T) has a direct dictionary + index wire encoding (COL_LOWCARD);
# nested LowCardinality (inside Array/Tuple) still materializes to T's full column (see the
# TODO on validateColumnBinaryWireSupportedType's LowCardinality branch).
run_roundtrip() {
    local type="$1"
    local select_expr="$2"
    local frame="${CLICKHOUSE_TMP}/04509_frame_$$.bin"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${select_expr} AS v FROM numbers(1) FORMAT ColumnBinary" > "${frame}"
    ${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04509;
CREATE TABLE t_04509 (v ${type}) ENGINE = Memory;
"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04509 FROM INFILE '${frame}' FORMAT ColumnBinary"
    ${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04509"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04509"
    rm -f "${frame}"
}

# Multiple rows with repeated values, to exercise real dictionary dedup on read.
run_roundtrip "LowCardinality(String)" \
    "arrayJoin(['a', 'b', 'a', 'c', 'b'])::LowCardinality(String)"

# LowCardinality(Nullable(String)): nullability lives inside the dictionary, not as a
# separate top-level Nullable wrapper — materializing must produce a real ColumnNullable
# so the existing null-map machinery picks it up.
run_roundtrip "LowCardinality(Nullable(String))" \
    "arrayJoin(['a', NULL, 'a', NULL, 'b'])::LowCardinality(Nullable(String))"

# A constant LowCardinality column.
run_roundtrip "LowCardinality(String)" \
    "'const'::LowCardinality(String)"

# Nested: Array(LowCardinality(...)) and Tuple(..., LowCardinality(...), ...).
run_roundtrip "Array(LowCardinality(String))" \
    "['a', 'b', 'a']::Array(LowCardinality(String))"

run_roundtrip "Tuple(LowCardinality(String), UInt64)" \
    "tuple('x', 42)::Tuple(LowCardinality(String), UInt64)"

# Direct-encoding proof: a low-cardinality column with many repeated values must produce a
# frame far smaller than encoding the same data as plain String (index bytes plus one small
# dictionary, not one full string value per row). Force a single output block/frame (fixed
# max_block_size, parallel formatting off) so the byte count is deterministic regardless of
# whatever block-size/formatting settings the test runner's random settings picked — each
# additional frame carries its own header+descriptor+dictionary overhead, which would
# otherwise make this an unstable assertion.
${CLICKHOUSE_CLIENT} --max_block_size 1000000 --output_format_parallel_formatting 0 \
    --query "SELECT toLowCardinality(toString(number % 5)) AS v FROM numbers(100000) FORMAT ColumnBinary" | wc -c
${CLICKHOUSE_CLIENT} --max_block_size 1000000 --output_format_parallel_formatting 0 \
    --query "SELECT toString(number % 5) AS v FROM numbers(100000) FORMAT ColumnBinary" | wc -c
