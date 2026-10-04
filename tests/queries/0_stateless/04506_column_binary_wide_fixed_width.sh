#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# Fixed-width types wider than 8 bytes (UUID, IPv6, Int128, Decimal128/256) and
# FixedString(N) of any length now round-trip via COL_FIXEDN (element width recovered
# as data_size/num_rows on read, since there's no per-width wire tag for them).
run_roundtrip() {
    local type="$1"
    local expr="$2"
    local frame="${CLICKHOUSE_TMP}/04506_frame_$$.bin"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${expr}::${type} AS v FROM numbers(3) FORMAT ColumnBinary" > "${frame}"
    ${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04506;
CREATE TABLE t_04506 (v ${type}) ENGINE = Memory;
"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04506 FROM INFILE '${frame}' FORMAT ColumnBinary"
    ${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_04506 ORDER BY v"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04506"
    rm -f "${frame}"
}

run_roundtrip "UUID" "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'"
run_roundtrip "IPv6" "'2001:db8::1'"
run_roundtrip "Int128" "170141183460469231731687303715884105727"
run_roundtrip "Decimal128(2)" "12345678901234567890.12"
run_roundtrip "Decimal256(4)" "123456789012345678901234567890123456789012345678.9012"
run_roundtrip "FixedString(5)" "'abcde'"

# Nested wide-fixed-width elements (no wire change needed there; only the validator's
# nested-width restriction had to be relaxed).
${CLICKHOUSE_CLIENT} --query "
SELECT ([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1')])::Array(UUID) AS a
FROM numbers(1) FORMAT ColumnBinary" > "${CLICKHOUSE_TMP}/04506_frame_arr.bin"
${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04506_arr;
CREATE TABLE t_04506_arr (a Array(UUID)) ENGINE = Memory;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04506_arr FROM INFILE '${CLICKHOUSE_TMP}/04506_frame_arr.bin' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT a FROM t_04506_arr"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04506_arr"
rm -f "${CLICKHOUSE_TMP}/04506_frame_arr.bin"

${CLICKHOUSE_CLIENT} --query "
SELECT tuple(toDecimal256(123.4567, 4), toUInt64(42))::Tuple(Decimal256(4), UInt64) AS t
FROM numbers(1) FORMAT ColumnBinary" > "${CLICKHOUSE_TMP}/04506_frame_tup.bin"
${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04506_tup;
CREATE TABLE t_04506_tup (t Tuple(Decimal256(4), UInt64)) ENGINE = Memory;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04506_tup FROM INFILE '${CLICKHOUSE_TMP}/04506_frame_tup.bin' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT t FROM t_04506_tup"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04506_tup"
rm -f "${CLICKHOUSE_TMP}/04506_frame_tup.bin"

${CLICKHOUSE_CLIENT} --query "
SELECT ['abcde', 'fghij']::Array(FixedString(5)) AS a
FROM numbers(1) FORMAT ColumnBinary" > "${CLICKHOUSE_TMP}/04506_frame_arrfs.bin"
${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04506_arrfs;
CREATE TABLE t_04506_arrfs (a Array(FixedString(5))) ENGINE = Memory;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04506_arrfs FROM INFILE '${CLICKHOUSE_TMP}/04506_frame_arrfs.bin' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT a FROM t_04506_arrfs"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04506_arrfs"
rm -f "${CLICKHOUSE_TMP}/04506_frame_arrfs.bin"
