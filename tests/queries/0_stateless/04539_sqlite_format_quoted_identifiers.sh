#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_quoted_identifiers_XXXXXX.sqlite")
TABLE_NAME='t"\name'
trap 'rm -f "$DB"' EXIT

# SQLite doubles embedded double quotes in identifiers and treats backslashes literally. Exercise both rules
# in table and column names across the output and input format paths.
${CLICKHOUSE_LOCAL} \
    --output_format_sqlite_table_name "$TABLE_NAME" \
    --query 'SELECT 1 AS `a"b`, 2 AS `c\\d` FORMAT SQLite' > "$DB"

echo 'SQLite schema column names:'
sqlite3 "$DB" "SELECT name FROM pragma_table_info('$TABLE_NAME') ORDER BY cid"

echo 'SQLite format roundtrip:'
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --output-format TSVWithNames \
    --input_format_sqlite_table_name "$TABLE_NAME" \
    --query 'SELECT * FROM table' < "$DB"
