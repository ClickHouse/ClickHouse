#!/usr/bin/env bash

export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function check()
{
    $CLICKHOUSE_CLIENT -q "SELECT id, $1(s) FROM ( SELECT number % 10 as id, uniqState(number) as s FROM ( SELECT number FROM system.numbers LIMIT 100 ) GROUP BY number ) GROUP BY id" 2>&1 | grep -v -P '^(Received exception from server|Code: 43)' ||:
}

stateFunctions=("uniqState" "uniqExactState" "uniqHLL12State" "uniqCombinedState" "uniqUpToState")  # "uniqThetaState" not tested because its availability depends on compilation options

for i1 in "${stateFunctions[@]}"
do
    check "$i1"
done
