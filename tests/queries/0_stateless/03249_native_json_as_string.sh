#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --output_format_native_write_json_as_string=1 --allow_experimental_json_type=1 -q "select toJSONString(map('key' || number % 3, 'value' || number))::JSON as json from numbers(5) format Native" | $CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --input-format=Native -q "select json, json.key0 from table";

