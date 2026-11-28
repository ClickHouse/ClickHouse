#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_schema_cache_table_name_XXXXXX.sqlite")
trap 'rm -f "$DB"' EXIT

sqlite3 "$DB" "CREATE TABLE first_table (id INTEGER NOT NULL, name TEXT);"
sqlite3 "$DB" "CREATE TABLE second_table (value REAL NOT NULL, payload TEXT);"

# Empty `input_format_sqlite_table_name` means infer schema from the first SQLite table
${CLICKHOUSE_LOCAL} --multiquery --query "
    SET schema_inference_use_cache_for_file = 1;

    SELECT 'Default table schema';
    DESC file('$DB', 'SQLite');

    SELECT 'First table schema';
    DESC file('$DB', 'SQLite')
    SETTINGS input_format_sqlite_table_name = 'first_table';

    SELECT 'Second table schema';
    DESC file('$DB', 'SQLite')
    SETTINGS input_format_sqlite_table_name = 'second_table';
" | cut -f1,2
