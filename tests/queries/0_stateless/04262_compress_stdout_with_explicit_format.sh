#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Test 1: --output-format with .gz redirect should produce gzip-compressed output
OUTPUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_output_format.gz"
rm -f "${OUTPUT_FILE}"
${CLICKHOUSE_CLIENT} --output-format TSV --query "SELECT 'from client with output-format'" > "${OUTPUT_FILE}"
gunzip "${OUTPUT_FILE}"
cat "${OUTPUT_FILE%.gz}"
rm -f "${OUTPUT_FILE%.gz}"

# Test 2: --vertical with .gz redirect should produce gzip-compressed output
OUTPUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_vertical.gz"
rm -f "${OUTPUT_FILE}"
${CLICKHOUSE_CLIENT} --vertical --query "SELECT 'from client with vertical' AS value" > "${OUTPUT_FILE}"
gunzip "${OUTPUT_FILE}"
cat "${OUTPUT_FILE%.gz}"
rm -f "${OUTPUT_FILE%.gz}"

# Test 3: clickhouse-local --output-format with .zst redirect should produce zstd-compressed output
OUTPUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local_output_format.zst"
rm -f "${OUTPUT_FILE}"
${CLICKHOUSE_LOCAL} --output-format TSV --query "SELECT 'from local with output-format'" > "${OUTPUT_FILE}"
zstd -d "${OUTPUT_FILE}" -o "${OUTPUT_FILE%.zst}" --rm -q
cat "${OUTPUT_FILE%.zst}"
rm -f "${OUTPUT_FILE%.zst}"

# Test 4: --format with compressed stdin should decompress correctly.
# Use --format (not --input-format) because --input-format in clickhouse-local
# sets the "table-data-format" config key, not "input-format", so it doesn't
# exercise the explicit-format branch in setDefaultFormatsAndCompressionFromConfiguration.
# Use a filename without .jsonl extension to prevent format inference from the fd name.
INPUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_input.gz"
rm -f "${INPUT_FILE}"
echo '{"x": 42}' | gzip > "${INPUT_FILE}"
${CLICKHOUSE_LOCAL} --format JSONEachRow --output-format TSV --query "SELECT x FROM table" < "${INPUT_FILE}"
rm -f "${INPUT_FILE}"
