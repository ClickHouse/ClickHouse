#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native ClickHouse Arrow IPC writer (output_format_arrow_use_native_writer=1) for both the
# ArrowStream and Arrow (file) formats over a wide type matrix. The data written natively must read
# back identically to the same data written by the Apache Arrow library writer.

GEN="SELECT
    toInt8(-number) AS i8, toUInt32(number) AS u32, toFloat64(number / 7) AS f64,
    (number % 2)::Bool AS b, toString(number) AS s, toFixedString(toString(number), 3) AS fs,
    toDate('2020-01-01') + number AS d, toDateTime64('2021-01-01 00:00:00', 3, 'UTC') + number AS dt64,
    toDecimal64(number * 1.25, 3) AS dec, range(number % 3) AS arr,
    (number, toString(number)) AS tup, map(toString(number), number) AS m,
    toLowCardinality(toString(number % 2)) AS lc,
    if(number % 3 = 0, NULL, number)::Nullable(UInt64) AS n
FROM numbers(6)"

WRITE_SETTINGS="output_format_arrow_string_as_string = 1, output_format_arrow_compression_method = 'none', max_block_size = 3"

for fmt in ArrowStream Arrow; do
    NATIVE_FILE="${CLICKHOUSE_TMP}/04317_native.${fmt}"
    LIBRARY_FILE="${CLICKHOUSE_TMP}/04317_library.${fmt}"

    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${NATIVE_FILE}', '${fmt}') ${GEN} SETTINGS ${WRITE_SETTINGS}, output_format_arrow_use_native_writer = 1"
    ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${LIBRARY_FILE}', '${fmt}') ${GEN} SETTINGS ${WRITE_SETTINGS}, output_format_arrow_use_native_writer = 0"

    echo "--- ${fmt}: native-written, read with native reader ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${NATIVE_FILE}', '${fmt}') ORDER BY u32 SETTINGS input_format_arrow_use_native_reader = 1"

    NATIVE=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${NATIVE_FILE}', '${fmt}') ORDER BY u32 SETTINGS input_format_arrow_use_native_reader = 1")
    NATIVE_VIA_LIB=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${NATIVE_FILE}', '${fmt}') ORDER BY u32 SETTINGS input_format_arrow_use_native_reader = 0")
    LIBRARY=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${LIBRARY_FILE}', '${fmt}') ORDER BY u32 SETTINGS input_format_arrow_use_native_reader = 0")

    echo "--- ${fmt}: native reader == library reader (on native file)? ---"
    [ "$NATIVE" = "$NATIVE_VIA_LIB" ] && echo "OK" || echo "MISMATCH"
    echo "--- ${fmt}: native-written == library-written? ---"
    [ "$NATIVE" = "$LIBRARY" ] && echo "OK" || echo "MISMATCH"

    rm -f "${NATIVE_FILE}" "${LIBRARY_FILE}"
done
