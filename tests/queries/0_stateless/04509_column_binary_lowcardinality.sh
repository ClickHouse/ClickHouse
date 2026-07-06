#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# LowCardinality(T) is currently supported by fully materializing to T's full column on
# write and rebuilding the dictionary via insertRangeFromFullColumn on read (see the TODO
# on validateColumnarV1SupportedType's LowCardinality branch for direct wire encoding).
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
