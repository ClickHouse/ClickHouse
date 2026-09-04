#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_archive_setting"
LITERAL_REL="${PREFIX}_literal.zip::data.csv"
LITERAL_ABS="${USER_FILES_PATH}/${LITERAL_REL}"
ARCHIVE_REL="${PREFIX}_archive.zip::data.csv"
ARCHIVE_ABS="${USER_FILES_PATH}/${PREFIX}_archive.zip"
PAYLOAD_DIR="${USER_FILES_PATH}/${PREFIX}_payload"
PAYLOAD="${PAYLOAD_DIR}/data.csv"

FILE_LITERAL_TABLE="${PREFIX}_file_literal"
FILE_ARCHIVE_TABLE="${PREFIX}_file_archive"
URL_LITERAL_TABLE="${PREFIX}_url_literal"
URL_ARCHIVE_TABLE="${PREFIX}_url_archive"

printf '11\n' > "$LITERAL_ABS"
mkdir -p "$PAYLOAD_DIR"
printf '22\n' > "$PAYLOAD"
zip -q -j "$ARCHIVE_ABS" "$PAYLOAD"

echo "--- File literal :: path keeps creation-time disabled interpretation ---"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=0 -q "CREATE TABLE ${FILE_LITERAL_TABLE} (x UInt64) ENGINE = File(CSV, '${LITERAL_REL}')"
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query LIKE '%allow_archive_path_syntax = false%' FROM system.tables WHERE database = currentDatabase() AND name = '${FILE_LITERAL_TABLE}'"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FILE_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${FILE_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 -q "ATTACH TABLE ${FILE_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FILE_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${FILE_LITERAL_TABLE}"

echo "--- File archive path keeps creation-time enabled interpretation ---"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 -q "CREATE TABLE ${FILE_ARCHIVE_TABLE} (x UInt64) ENGINE = File(CSV, '${ARCHIVE_REL}')"
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query LIKE '%allow_archive_path_syntax = true%' FROM system.tables WHERE database = currentDatabase() AND name = '${FILE_ARCHIVE_TABLE}'"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FILE_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${FILE_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=0 -q "ATTACH TABLE ${FILE_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FILE_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${FILE_ARCHIVE_TABLE}"

echo "--- URL file dispatch keeps literal :: interpretation ---"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=0 -q "CREATE TABLE ${URL_LITERAL_TABLE} (x UInt64) ENGINE = URL('file://${LITERAL_ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query LIKE '%allow_archive_path_syntax = false%' FROM system.tables WHERE database = currentDatabase() AND name = '${URL_LITERAL_TABLE}'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${URL_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 -q "ATTACH TABLE ${URL_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${URL_LITERAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${URL_LITERAL_TABLE}"

echo "--- URL file dispatch keeps archive interpretation ---"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 -q "CREATE TABLE ${URL_ARCHIVE_TABLE} (x UInt64) ENGINE = URL('file://${ARCHIVE_ABS}::data.csv', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query LIKE '%allow_archive_path_syntax = true%' FROM system.tables WHERE database = currentDatabase() AND name = '${URL_ARCHIVE_TABLE}'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${URL_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=0 -q "ATTACH TABLE ${URL_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${URL_ARCHIVE_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${URL_ARCHIVE_TABLE}"

rm -f "$LITERAL_ABS" "$ARCHIVE_ABS" "$PAYLOAD"
rmdir "$PAYLOAD_DIR"
