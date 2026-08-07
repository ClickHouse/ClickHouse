#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select '{\"a\" : 42}'::JSON as data format Native settings enable_json_type=1, output_format_native_write_json_as_string=1, engine_file_truncate_on_insert=1, output_format_json_quote_64bit_integers=0" | $CLICKHOUSE_LOCAL --input-format Native -q "select JSONAllPathsWithTypes(data) from table"

