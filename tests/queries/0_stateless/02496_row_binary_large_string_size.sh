#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

printf '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' | $CLICKHOUSE_LOCAL --format_binary_max_string_size=100000 --input-format=RowBinary --structure='s String' -q "select * from table" 2>&1 | grep -q -F "TOO_LARGE_STRING_SIZE" && echo "OK" || echo FAIL""
