#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --explain_query_plan_default=legacy"
${CLICKHOUSE_FORMAT} --query "SELECT explain FROM (EXPLAIN AST SELECT * FROM system.numbers)"
