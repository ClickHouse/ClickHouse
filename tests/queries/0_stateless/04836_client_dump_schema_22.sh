#!/usr/bin/env bash

# --dump-schema reads the catalog with its own queries, which must return every row as is. The settings that reshape the
# result of a SELECT apply to every query of the session: the query-construction settings (`filter`, `select`, `order`,
# `sort`, `page` with `limit`), `additional_result_filter`, `additional_table_filters` and an output format `Null`. The
# dump resets them for its own queries, in clickhouse-local and in clickhouse-client.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Every one of the two tables is dumped whatever the setting, which `filter` and `additional_table_filters` would drop.
FILTER="position(toString(tuple(*)), 'must_be_exported') = 0"
TABLE_FILTERS="{'system.tables': 'name != \\'must_be_exported\\''}"
# The server has the tables of other tests too, so its filter names the test database.
CLIENT_TABLE_FILTERS="{'system.tables': 'NOT (database = currentDatabase() AND name = \\'must_be_exported\\')'}"

$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.keep_t (x UInt8) ENGINE = Memory;
    CREATE TABLE d.must_be_exported (x UInt8) ENGINE = Memory;"
dump_local() {
    local name=$1
    shift
    $CLICKHOUSE_LOCAL --path "$work/src" "$@" --dump-schema d > "$work/$name.sql" 2> "$work/$name.err"
    echo "local $name: rc=$? tables=$(grep -c '^CREATE TABLE' "$work/$name.sql")"
}
dump_local filter --filter "$FILTER"
dump_local select --select 1
dump_local order --order '1 DESC'
dump_local sort --sort '-1'
dump_local page --limit 10 --page 2
dump_local additional_result_filter --additional_result_filter 0
dump_local additional_table_filters --additional_table_filters "$TABLE_FILTERS"
dump_local format_null --format Null
dump_local output_format_null --output_format Null

$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE keep_t (x UInt8) ENGINE = Memory;
    CREATE TABLE must_be_exported (x UInt8) ENGINE = Memory;"
dump_client() {
    local name=$1
    shift
    $CLICKHOUSE_CLIENT "$@" --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/$name.sql" 2> "$work/$name.err"
    echo "client $name: rc=$? tables=$(grep -c '^CREATE TABLE' "$work/$name.sql")"
}
dump_client filter --filter "$FILTER"
dump_client select --select 1
dump_client page --limit 10 --page 2
dump_client additional_table_filters --additional_table_filters "$CLIENT_TABLE_FILTERS"
