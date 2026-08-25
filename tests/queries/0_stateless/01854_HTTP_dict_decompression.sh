#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

python3 "$CURDIR"/01854_HTTP_dict_decompression.python

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY test_table_select"
