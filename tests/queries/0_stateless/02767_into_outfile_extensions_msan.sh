#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --explain_query_plan_default='legacy'"
out="explain1.$CLICKHOUSE_TEST_UNIQUE_NAME.out"
# only EXPLAIN triggers the problem under MSan
$CLICKHOUSE_CLIENT --enable_analyzer=0 -q "explain select * from numbers(1) into outfile '$out'"
cat "$out"
rm -f "$out"
