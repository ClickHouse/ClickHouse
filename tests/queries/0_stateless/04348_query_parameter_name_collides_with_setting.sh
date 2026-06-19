#!/usr/bin/env bash

# Regression: a query parameter whose name collides with a real setting. The HTTP "table as file"
# feature promoted `format` / `database` / `filter` / `select` / ... to first-class settings, so a
# `--param_format=...` is stored as a typed setting field rather than a custom one. The server
# reconstructs the parameter map (`Settings::toNameToNameMap`) by the field's exact type
# (`isCustom()`), not by guessing "starts with a quote -> SQL-quoted" — the latter used to corrupt
# typed values that legitimately start with `'` (e.g. `--param_format="'abc"` threw
# CANNOT_PARSE_QUOTED_STRING). Custom (non-colliding) parameters keep their SQL-quoted decoding.

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
echo "-- non-colliding custom param, value starting with a quote still works"
${CLICKHOUSE_CLIENT} --param_myparam="'hello" -q "SELECT {myparam:String}"
echo "-- non-colliding custom param, ordinary value still works"
${CLICKHOUSE_CLIENT} --param_myparam="default" -q "SELECT {myparam:String}"
