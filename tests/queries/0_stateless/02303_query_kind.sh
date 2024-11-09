#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    "--allow_experimental_analyzer=1"
)

function run_query()
{
    echo "clickhouse-client $*"
    $CLICKHOUSE_CLIENT "$@"

    echo "clickhouse-local $*"
    $CLICKHOUSE_LOCAL "$@"
}
run_query "${opts[@]}" --query_kind secondary_query -q "explain plan header=1 select toString(dummy) as dummy from system.one group by dummy"
run_query "${opts[@]}" --query_kind initial_query -q "explain plan header=1 select toString(dummy) as dummy from system.one group by dummy"
