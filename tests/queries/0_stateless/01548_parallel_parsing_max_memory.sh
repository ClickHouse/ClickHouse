#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

yes http://foobarfoobarfoobarfoobarfoobarfoobarfoobar.com | head -c1G > 1g.csv

$CLICKHOUSE_LOCAL --stacktrace --input_format_parallel_parsing=1 --max_memory_usage=100Mi -q "select count() from file('1g.csv', 'TSV', 'URL String')"