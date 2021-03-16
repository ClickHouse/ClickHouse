#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function execute_query()
{
    if [ $# -eq 0 ]; then
        echo "execute: default"
    else
        echo "execute: $*"
    fi
    ${CLICKHOUSE_CLIENT} "$@" --format CSVWithNames -q "SELECT 1 AS foo"
}

execute_query # default -- complete
execute_query --stage fetch_columns
execute_query --stage with_mergeable_state
execute_query --stage with_mergeable_state_after_aggregation
execute_query --stage complete
