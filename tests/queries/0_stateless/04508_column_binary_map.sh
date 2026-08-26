#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# Map(K, V) is Array(Tuple(K, V)) under the hood; no dedicated wire tag needed, it's
# unwrapped to that at every touch point and goes through the existing Array/Tuple path.
run_roundtrip() {
    local type="$1"
    local select_expr="$2"
    local frame="${CLICKHOUSE_TMP}/04508_frame_$$.bin"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${select_expr} AS v FROM numbers(1) FORMAT ColumnBinary" > "${frame}"
    ${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04508;
CREATE TABLE t_04508 (v ${type}) ENGINE = Memory;
"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04508 FROM INFILE '${frame}' FORMAT ColumnBinary"
    ${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04508"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04508"
    rm -f "${frame}"
}

run_roundtrip "Map(String, UInt64)" \
    "map('a', 1, 'b', 2)::Map(String, UInt64)"

run_roundtrip "Map(String, UInt64)" \
    "map()::Map(String, UInt64)"

# Nested: Array(Map(...)) and Tuple(..., Map(...), ...).
run_roundtrip "Array(Map(String, UInt64))" \
    "[map('a', 1), map(), map('b', 2, 'c', 3)]::Array(Map(String, UInt64))"

run_roundtrip "Tuple(Map(String, UInt64), String)" \
    "tuple(map('x', 42), 'y')::Tuple(Map(String, UInt64), String)"
