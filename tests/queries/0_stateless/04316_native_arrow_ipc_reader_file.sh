#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native ClickHouse Arrow IPC reader on the file format (Arrow), which uses the footer for
# random access to record batches and dictionary batches. Data is produced by the Apache Arrow
# library writer (multiple row groups -> multiple record batches); the native reader must match the
# library reader exactly, including the metadata-only count path.

DATA_FILE="${CLICKHOUSE_TMP}/04316_file.arrow"

${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}', 'Arrow')
SELECT
    toInt32(number) AS i,
    toString(number) AS s,
    range(number % 4) AS arr,
    toLowCardinality(toString(number % 3)) AS lc
FROM numbers(10)
SETTINGS output_format_arrow_string_as_string = 1,
         output_format_arrow_compression_method = 'none',
         output_format_arrow_low_cardinality_as_dictionary = 1,
         max_block_size = 3
"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'Arrow') SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- data (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'Arrow') ORDER BY i SETTINGS input_format_arrow_use_native_reader = 1"

echo "--- native vs library: data identical? ---"
ND=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'Arrow') ORDER BY i SETTINGS input_format_arrow_use_native_reader = 1")
LD=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'Arrow') ORDER BY i SETTINGS input_format_arrow_use_native_reader = 0")
[ "$ND" = "$LD" ] && echo "OK" || echo "MISMATCH"

echo "--- count only (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${DATA_FILE}', 'Arrow') SETTINGS input_format_arrow_use_native_reader = 1"

rm -f "${DATA_FILE}"
