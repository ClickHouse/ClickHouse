#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native ClickHouse Arrow IPC reader on nested types (Array, Tuple, Map, nested Array) and
# LowCardinality (Arrow dictionary) columns, including LowCardinality(Nullable(...)). Data is produced
# by the Apache Arrow library writer; the native reader must match the library reader exactly.

DATA_FILE="${CLICKHOUSE_TMP}/04315_nested.arrows"

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream')
SELECT
    range(number) AS arr,
    arrayMap(x -> if(x % 2 = 0, NULL, x), range(number))::Array(Nullable(UInt32)) AS arr_nullable,
    (number, toString(number)) AS tup,
    map(toString(number), number, 'k', number * 2) AS m,
    toLowCardinality(toString(number % 3)) AS lc,
    toLowCardinality(if(number % 2 = 0, NULL, toString(number)))::LowCardinality(Nullable(String)) AS lc_null,
    [[toUInt8(1), 2], [3]]::Array(Array(UInt8)) AS nested_arr
FROM numbers(5)
SETTINGS output_format_arrow_string_as_string = 1,
         output_format_arrow_compression_method = 'none',
         output_format_arrow_low_cardinality_as_dictionary = 1
"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- data (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY arr SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- native vs library: schema identical? ---"
NS=$(${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1")
LS=$(${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 0")
[ "$NS" = "$LS" ] && echo "OK" || echo "MISMATCH"

echo "--- native vs library: data identical? ---"
ND=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY arr SETTINGS input_format_arrow_use_native_reader = 1")
LD=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY arr SETTINGS input_format_arrow_use_native_reader = 0")
[ "$ND" = "$LD" ] && echo "OK" || echo "MISMATCH"

rm -f "${DATA_FILE}"
