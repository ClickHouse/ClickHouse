#!/usr/bin/env bash

# Tags: no-fasttest, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

partial_type="JSON(max_dynamic_paths=10, SHARED REGEXP 'foo')"
full_type="JSON(max_dynamic_paths=10, shared_regexp_use_partial_match=0, SHARED REGEXP 'foo')"
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

write_native_subobject()
{
    $CLICKHOUSE_LOCAL --enable_json_type=1 -q \
        "SELECT j.^outer AS j
         FROM format(
             JSONEachRow,
             'j JSON(max_dynamic_paths=10, SHARED REGEXP \\'^outer[.]forced$\\')',
             '{\"j\":{\"outer\":{\"forced\":1,\"keep\":2},\"forced\":3}}')
         FORMAT Native
         SETTINGS
             output_format_native_write_json_as_string=0,
             output_format_native_use_flattened_dynamic_and_json_serialization=1,
             output_format_native_encode_types_in_binary_format=1"
}

printf '%s\n' 'JSONEachRow partial'
printf '%s\n' "$json_row" |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --structure="j $partial_type" --input-format=JSONEachRow \
        --type_json_use_partial_match_to_skip_paths_by_regexp=0 -q "$show_paths_query"

printf '%s\n' 'JSONEachRow full'
printf '%s\n' "$json_row" |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --structure="j $full_type" --input-format=JSONEachRow \
        --type_json_use_partial_match_to_skip_paths_by_regexp=1 -q "$show_paths_query"

printf '%s\n' 'RowBinary partial'
write_row_binary |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --structure="j $partial_type" --input-format=RowBinary \
        --type_json_use_partial_match_to_skip_paths_by_regexp=0 -q "$show_paths_query"

printf '%s\n' 'RowBinary full'
write_row_binary |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --structure="j $full_type" --input-format=RowBinary \
        --type_json_use_partial_match_to_skip_paths_by_regexp=1 -q "$show_paths_query"

# The next two cases round-trip both the policy and the data through Native's flattened encoding.
printf '%s\n' 'Native flattened partial'
write_native "$partial_type" 0 |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --input-format=Native -q "$show_paths_query"

printf '%s\n' 'Native flattened full'
write_native "$full_type" 1 |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --input-format=Native -q "$show_paths_query"

# Also exercise insertion of a flattened Native column into a destination with a new policy.
printf '%s\n' 'Native flattened conversion'
write_native 'JSON(max_dynamic_paths=10)' 0 |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --input-format=Native -m -q \
        "CREATE TABLE dst (j $partial_type) ENGINE=Memory;
         INSERT INTO dst SELECT j FROM table;
         SELECT arraySort(JSONDynamicPaths(j)), arraySort(JSONSharedDataPaths(j)) FROM dst"

# Native's binary type encoding must preserve the derived sub-object's root prefix. Constructing a
# fresh value from the decoded type proves that `forced` is still matched as `outer.forced`.
printf '%s\n' 'Native flattened sub-object prefix'
write_native_subobject |
    $CLICKHOUSE_LOCAL --enable_json_type=1 --input-format=Native \
        --input_format_native_decode_types_in_binary_format=1 -q \
        "WITH CAST('{\"forced\":10,\"keep\":20}', toTypeName(j)) AS fresh
         SELECT
             position(toTypeName(j), 'shared_regexp_path_prefix=\\'outer.\\'') > 0,
             arraySort(JSONDynamicPaths(fresh)),
             arraySort(JSONSharedDataPaths(fresh))
         FROM table"
