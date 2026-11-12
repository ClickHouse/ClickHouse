#!/usr/bin/env bash

# Regression: a query parameter whose name collides with a real setting. The HTTP "table as file"
# feature promoted `format` / `database` / `filter` / `select` / `page` / ... to first-class
# settings. Query parameters travel over the native protocol in their own wire section: the client
# writes each parameter as a *custom* (string-valued) field via `writeQueryParameters`, and the
# server reads them back with `readQueryParameters`, SQL-unquoting each value. Because parameters
# never go through the settings type system on either side, a parameter whose name collides with a
# built-in setting is never parsed as that setting's type. For a `String` setting like `format`
# typed parsing would (used to) corrupt values that legitimately start with `'`
# (e.g. `--param_format="'abc"` threw CANNOT_PARSE_QUOTED_STRING); for a numeric setting like `page`
# (a `Double`) typed parsing would reject a non-numeric value altogether (`--param_page=foo`) or
# normalize a numeric-looking string. Storing and reading them as plain name/value pairs makes the
# original string round-trip intact.
#
# A parameter whose name is a setting *alias* (e.g. `replication_alter_partitions_sync`, an alias of
# `alter_sync`) is a stronger case: both `writeQueryParameters` (client side) and
# `readQueryParameters` (server side) transport the parameter under the user's chosen name, NOT the
# alias-resolved canonical name — otherwise `{replication_alter_partitions_sync:String}` cannot find
# a value stored under `alter_sync` and substitution fails.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "-- colliding param 'format', value starting with a quote"
${CLICKHOUSE_CLIENT} --param_format="'abc" -q "SELECT {format:String}"
echo "-- colliding param 'database', value starting with a quote"
${CLICKHOUSE_CLIENT} --param_database="'xyz" -q "SELECT {database:String}"
echo "-- colliding param 'filter', value starting with a quote"
${CLICKHOUSE_CLIENT} --param_filter="'a > 0" -q "SELECT {filter:String}"
echo "-- colliding param 'format', ordinary value still works"
${CLICKHOUSE_CLIENT} --param_format="JSONEachRow" -q "SELECT {format:String}"
echo "-- colliding param 'page' (a Double setting), non-numeric string value"
${CLICKHOUSE_CLIENT} --param_page="foo" -q "SELECT {page:String}"
echo "-- colliding param 'page' (a Double setting), numeric-looking string preserved (no normalization)"
${CLICKHOUSE_CLIENT} --param_page="05" -q "SELECT {page:String}"
echo "-- colliding param 'limit' (a Double setting), non-numeric string value"
${CLICKHOUSE_CLIENT} --param_limit="x>1" -q "SELECT {limit:String}"
echo "-- colliding param 'replication_alter_partitions_sync' (alias of 'alter_sync'), name preserved through alias resolution"
${CLICKHOUSE_CLIENT} --param_replication_alter_partitions_sync="foo" -q "SELECT {replication_alter_partitions_sync:String}"
echo "-- non-colliding custom param, value starting with a quote still works"
${CLICKHOUSE_CLIENT} --param_myparam="'hello" -q "SELECT {myparam:String}"
echo "-- non-colliding custom param, ordinary value still works"
${CLICKHOUSE_CLIENT} --param_myparam="default" -q "SELECT {myparam:String}"
