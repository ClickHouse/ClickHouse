#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native ClickHouse Arrow IPC reader (input_format_arrow_use_native_reader=1) for the
# ArrowStream format on flat (non-nested), uncompressed data. The data is produced by the Apache
# Arrow library writer; the native reader must produce exactly the same result as the library reader.

DATA_FILE="${CLICKHOUSE_TMP}/04314_flat.arrows"

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream')
SELECT
    toInt8(-number) AS i8,
    toUInt16(number) AS u16,
    toInt64(number * 1000000) AS i64,
    toFloat32(number / 3) AS f32,
    toFloat64(number / 7) AS f64,
    (number % 2)::Bool AS b,
    toString(number) AS s,
    toFixedString(toString(number), 4) AS fs,
    toDecimal64(number * 1.25, 3) AS dec64,
    toDecimal128(number, 6) AS dec128,
    toDate32('2021-01-01') + number AS d32,
    toDateTime64('2021-01-01 00:00:00', 3, 'UTC') + number AS dt64,
    if(number % 3 = 0, NULL, number)::Nullable(UInt64) AS nullable_n
FROM numbers(7)
SETTINGS output_format_arrow_string_as_string = 1,
         output_format_arrow_fixed_string_as_fixed_byte_array = 1,
         output_format_arrow_compression_method = 'none'
"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- data (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY u16 SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- native vs library: identical? ---"
NATIVE=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY u16 SETTINGS input_format_arrow_use_native_reader = 1")
LIBRARY=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY u16 SETTINGS input_format_arrow_use_native_reader = 0")
[ "$NATIVE" = "$LIBRARY" ] && echo "OK" || echo "MISMATCH"

echo "--- count only (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- empty (0 rows, native) ---"
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') SELECT toInt32(number) AS i, toString(number) AS s FROM numbers(0) SETTINGS output_format_arrow_compression_method = 'none', output_format_arrow_string_as_string = 1, engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1"

rm -f "${DATA_FILE}"
