#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_XXXXXX.sqlite")
CUSTOM_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_custom_XXXXXX.sqlite")
FIRST_TABLE_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_first_table_XXXXXX.sqlite")
DIRECT_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_direct_XXXXXX.sqlite")
rm -f "$DIRECT_DB"
trap 'rm -f "$DB" "$CUSTOM_DB" "$FIRST_TABLE_DB" "$DIRECT_DB"' EXIT

STRUCTURE="id UInt64, name String, amount Decimal64(2), created DateTime, value Nullable(String)"

echo "Format registration"
${CLICKHOUSE_LOCAL} --query "SELECT name, is_input, is_output, supports_append FROM system.formats WHERE name = 'SQLite' FORMAT TSV"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        toUInt64(9223372036854775808) AS id,
        'hello' AS name,
        toDecimal64(123.45, 2) AS amount,
        toDateTime('2020-01-02 03:04:05', 'UTC') AS created,
        CAST(NULL, 'Nullable(String)') AS value
    UNION ALL
    SELECT
        toUInt64(42),
        'world',
        toDecimal64(-7.50, 2),
        toDateTime('2021-05-06 07:08:09', 'UTC'),
        CAST('text', 'Nullable(String)')
    FORMAT SQLite" > "$DB"

echo "Roundtrip default table"
${CLICKHOUSE_LOCAL} \
    --structure "$STRUCTURE" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT id, name, amount, created, value FROM table ORDER BY id" < "$DB"

echo "Local file table function"
${CLICKHOUSE_LOCAL} --query "SELECT id, name FROM file('$DB', 'SQLite') ORDER BY id"

echo "Local file output"
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('$DIRECT_DB', 'SQLite', 'id UInt64, name String') SELECT number AS id, concat('name_', toString(number)) AS name FROM numbers(3)"
${CLICKHOUSE_LOCAL} --query "SELECT id, name FROM file('$DIRECT_DB', 'SQLite') ORDER BY id"

echo "Local file output no append"
${CLICKHOUSE_LOCAL} --query "SELECT 10 AS x FORMAT SQLite SETTINGS output_format_sqlite_table_name = 't0'" > "$DIRECT_DB"
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('$DIRECT_DB', 'SQLite', 'y UInt8') SELECT 2 AS y SETTINGS output_format_sqlite_table_name = 't1'" 2>&1 | grep -o "CANNOT_APPEND_TO_FILE"
${CLICKHOUSE_LOCAL} --query "SELECT name FROM file('$DIRECT_DB', 'SQLite', 'name String') SETTINGS input_format_sqlite_table_name = 'sqlite_master'"

echo "Local file output truncate"
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('$DIRECT_DB', 'SQLite', 'y UInt8') SELECT 2 AS y SETTINGS output_format_sqlite_table_name = 't1', engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} --query "SELECT name FROM file('$DIRECT_DB', 'SQLite', 'name String') SETTINGS input_format_sqlite_table_name = 'sqlite_master'"
${CLICKHOUSE_LOCAL} --query "SELECT y FROM file('$DIRECT_DB', 'SQLite', 'y UInt8') SETTINGS input_format_sqlite_table_name = 't1'"

echo "Schema inference"
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --output-format TSV \
    --query "DESCRIBE TABLE table" < "$DB" | cut -f1,2

echo "NULL as default"
${CLICKHOUSE_LOCAL} \
    --structure "id UInt64, value String" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT id, length(value), value = '' FROM table ORDER BY id" < "$DB"

echo "NULL as default disabled"
${CLICKHOUSE_LOCAL} \
    --structure "id UInt64, value String" \
    --input-format SQLite \
    --output-format TSV \
    --input_format_null_as_default 0 \
    --query "SELECT id, value FROM table ORDER BY id" < "$DB" 2>&1 \
    | grep -o "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN" | head -1

${CLICKHOUSE_LOCAL} --query "
    SELECT 7 AS id, 'first' AS name
    FORMAT SQLite
    SETTINGS output_format_sqlite_table_name = 'first_table'" > "$FIRST_TABLE_DB"

echo "Read first table by default"
${CLICKHOUSE_LOCAL} \
    --structure "id UInt8, name String" \
    --input-format SQLite \
    --output-format TSV \
    --query "SELECT * FROM table" < "$FIRST_TABLE_DB"

${CLICKHOUSE_LOCAL} --query "
    SELECT 1 AS x, 'custom' AS y
    FORMAT SQLite
    SETTINGS output_format_sqlite_table_name = 'result'" > "$CUSTOM_DB"

echo "Custom table name"
${CLICKHOUSE_LOCAL} \
    --structure "x UInt8, y String" \
    --input-format SQLite \
    --output-format TSV \
    --input_format_sqlite_table_name result \
    --query "SELECT * FROM table" < "$CUSTOM_DB"
