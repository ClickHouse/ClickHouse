#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# Nullable(T) nested inside Array/Tuple: complexDataSize/writeComplexData/decode prepend
# a u8 null_map[n] before T's own layout. Rows mix NULL and non-NULL elements to catch
# null-map bit-position bugs.
run_roundtrip() {
    local type="$1"
    local select_expr="$2"
    local frame="${CLICKHOUSE_TMP}/04507_frame_$$.bin"
    ${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --query "SELECT ${select_expr} AS v FROM numbers(1) FORMAT ColumnBinary" > "${frame}"
    ${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --multiquery --query "
DROP TABLE IF EXISTS t_04507;
CREATE TABLE t_04507 (v ${type}) ENGINE = Memory;
"
    ${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --query "INSERT INTO t_04507 FROM INFILE '${frame}' FORMAT ColumnBinary"
    ${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04507"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04507"
    rm -f "${frame}"
}

run_roundtrip "Array(Nullable(String))" \
    "[NULL, 'a', NULL, 'bb']::Array(Nullable(String))"

run_roundtrip "Tuple(Nullable(UInt64), String)" \
    "tuple(NULL, 'x')::Tuple(Nullable(UInt64), String)"

run_roundtrip "Tuple(Nullable(UInt64), String)" \
    "tuple(42, 'y')::Tuple(Nullable(UInt64), String)"

# Nullable(Tuple(...)) as an array element (needs both phase 1 and phase 3 together).
run_roundtrip "Array(Nullable(Tuple(UInt64)))" \
    "[NULL, tuple(7)]::Array(Nullable(Tuple(UInt64)))"
