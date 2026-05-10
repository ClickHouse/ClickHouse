#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_projection_XXXXXX.sqlite")
trap 'rm -f "$DB"' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT toUInt8(1) AS id, 'not an integer' AS bad_integer, 'hello' AS payload
    UNION ALL
    SELECT toUInt8(2), 'still not an integer', 'world'
    FORMAT SQLite
    SETTINGS output_format_sqlite_table_name = 'data'" > "$DB"

echo "Read requested columns only"
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --input_format_sqlite_table_name data \
    --structure "id UInt64, bad_integer UInt64, payload String" \
    --query "SELECT id, payload FROM table ORDER BY id" < "$DB"

echo "Read rejected column"
if ${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --input_format_sqlite_table_name data \
    --structure "id UInt64, bad_integer UInt64, payload String" \
    --query "SELECT bad_integer FROM table" < "$DB" > /dev/null 2> "$CLICKHOUSE_TMP/sqlite_format_projection_error.log"
then
    echo "Fail"
else
    grep -o "UNEXPECTED_DATA_AFTER_PARSED_VALUE" "$CLICKHOUSE_TMP/sqlite_format_projection_error.log"
fi
