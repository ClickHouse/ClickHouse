#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT"

echo "select 1 format CSV settings max_execution_time = 0.001" | $format
echo "select 1 settings max_execution_time = 0.001 format CSV" | $format
echo "select 1 UNION ALL Select 2 format CSV settings max_execution_time = 0.001" | $format

# I don't think having multiple settings makes sense, but it's supported so test that it still works
echo "select 1 settings max_threads=1 UNION ALL select 2 settings max_execution_time=2 format Null" | $format
