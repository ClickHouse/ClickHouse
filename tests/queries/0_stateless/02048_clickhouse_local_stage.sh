#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    "--enable_analyzer=1"
)

function execute_query()
{
    if [ $# -eq 0 ]; then
        echo "execute: default"
    else
        echo "execute: $*"
    fi
    ${CLICKHOUSE_LOCAL} "$@" --format CSVWithNames -q "SELECT 1 AS foo"
}

execute_query "${opts[@]}" # default -- complete
execute_query "${opts[@]}" --stage fetch_columns
execute_query "${opts[@]}" --stage with_mergeable_state
execute_query "${opts[@]}" --stage with_mergeable_state_after_aggregation
execute_query "${opts[@]}" --stage complete
