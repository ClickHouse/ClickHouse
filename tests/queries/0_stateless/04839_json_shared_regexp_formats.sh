#!/usr/bin/env bash

# Tags: no-fasttest, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

partial_type="JSON(max_dynamic_paths=10, SHARED REGEXP 'foo')"
json_row='{"j":{"foo":1,"foobar":2,"keep":3}}'

show_paths_query='SELECT arraySort(JSONDynamicPaths(j)), arraySort(JSONSharedDataPaths(j)) FROM table'

write_row_binary()
{
    $CLICKHOUSE_LOCAL --enable_json_type=1 -q \
        "SELECT '{\"foo\":1,\"foobar\":2,\"keep\":3}'::JSON(max_dynamic_paths=10) AS j
         FORMAT RowBinary
         SETTINGS output_format_binary_write_json_as_string=0"
}

write_native()
{
    local type=$1
    local ambient_setting=$2
    $CLICKHOUSE_LOCAL --enable_json_type=1 -q \
        "SELECT CAST('{\"foo\":1,\"foobar\":2,\"keep\":3}' AS $type) AS j
         FORMAT Native
         SETTINGS
             output_format_native_write_json_as_string=0,
             output_format_native_use_flattened_dynamic_and_json_serialization=1,
             type_json_use_partial_match_to_skip_paths_by_regexp=$ambient_setting"
}

printf '%s\n' 'JSONEachRow partial'
printf '%s\n' "$json_row" |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --structure="j $partial_type" --input-format=JSONEachRow \
        --type_json_use_partial_match_to_skip_paths_by_regexp=0 -q "$show_paths_query"

printf '%s\n' 'RowBinary partial'
write_row_binary |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --structure="j $partial_type" --input-format=RowBinary \
        --type_json_use_partial_match_to_skip_paths_by_regexp=0 -q "$show_paths_query"

# Round-trip both the rules and the data through Native's flattened encoding.
printf '%s\n' 'Native flattened partial'
write_native "$partial_type" 0 |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --input-format=Native -q "$show_paths_query"

# Insert a flattened Native column into a destination with the rules.
printf '%s\n' 'Native flattened conversion'
write_native 'JSON(max_dynamic_paths=10)' 0 |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --input-format=Native -m -q \
        "CREATE TABLE dst (j $partial_type) ENGINE=Memory;
         INSERT INTO dst SELECT j FROM table;
         SELECT arraySort(JSONDynamicPaths(j)), arraySort(JSONSharedDataPaths(j)) FROM dst"
