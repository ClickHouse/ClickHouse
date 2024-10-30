#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select toJSONString(map('key' || number % 3, 'value' || number))::JSON from numbers(5) format RowBinary settings output_format_binary_write_json_as_string=1" | $CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --input_format_binary_read_json_as_string=1 --structure='json JSON' --input-format=RowBinary -q "select json, json.key0 from table";

$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select toJSONString(map('key' || number % 3, 'value' || number))::JSON from numbers(5) format RowBinary settings output_format_binary_write_json_as_string=1" | $CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --input_format_binary_read_json_as_string=1 --structure='json String' --input-format=RowBinary -q "select json from table";

