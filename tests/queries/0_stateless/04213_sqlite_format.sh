#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_XXXXXX.sqlite")
CUSTOM_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_custom_XXXXXX.sqlite")
trap 'rm -f "$DB" "$CUSTOM_DB"' EXIT

STRUCTURE="id UInt64, name String, amount Decimal64(2), created DateTime, value Nullable(String)"

echo "Format registration"
${CLICKHOUSE_LOCAL} --query "SELECT name, is_input, is_output FROM system.formats WHERE name = 'SQLite' FORMAT TSV"

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

echo "Schema inference"
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --output-format TSV \
    --query "DESCRIBE TABLE table" < "$DB" | cut -f1,2

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
