#!/usr/bin/env bash

# Regression: a query parameter whose name collides with a real setting. The HTTP "table as file"
# feature promoted `format` / `database` / `filter` / `select` / `page` / ... to first-class
# settings. Query parameters are transported through the `Settings` serialization, so the client
# now stores every parameter as a *custom* (string-valued) field (`Settings::setCustom`) rather than
# via typed `set`: a parameter whose name collides with a built-in setting must not be parsed as that
# setting's type. For a `String` setting like `format` that would (used to) corrupt values that
# legitimately start with `'` (e.g. `--param_format="'abc"` threw CANNOT_PARSE_QUOTED_STRING); for a
# numeric setting like `page` (a `Double`) typed parsing would reject a non-numeric value altogether
# (`--param_page=foo`) or normalize a numeric-looking string. The server honors the wire CUSTOM flag
# even for a colliding name (`BaseSettings::read`) and reconstructs the map (`toNameToNameMap`) by the
# field's `isCustom()` type, SQL-unquoting custom values — so the original string round-trips intact.
#
# A parameter whose name is a setting *alias* (e.g. `enable_analyzer`, an alias of
# `allow_experimental_analyzer`) is a stronger case: both `Settings::setCustom` (client side) and
# `BaseSettings::read` (server side) must store the custom field under the user's chosen name, NOT the
# alias-resolved canonical name — otherwise `{enable_analyzer:String}` cannot find a value stored under
# `allow_experimental_analyzer` and substitution fails.

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
echo "-- colliding param 'enable_analyzer' (alias of 'allow_experimental_analyzer'), name preserved through alias resolution"
${CLICKHOUSE_CLIENT} --param_enable_analyzer="foo" -q "SELECT {enable_analyzer:String}"
echo "-- non-colliding custom param, value starting with a quote still works"
${CLICKHOUSE_CLIENT} --param_myparam="'hello" -q "SELECT {myparam:String}"
echo "-- non-colliding custom param, ordinary value still works"
${CLICKHOUSE_CLIENT} --param_myparam="default" -q "SELECT {myparam:String}"
