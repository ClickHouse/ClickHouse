#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('data_binary/02496_large_string_size.bin', auto, 's String') settings input_format_max_binary_string_size=100000" 2>&1 | grep -q -F "TOO_LARGE_STRING_SIZE" && echo "OK" || echo "FAIL"

