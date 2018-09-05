#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

exception_pattern="Value passed to 'throwIf' function is non zero"

${CLICKHOUSE_CLIENT} --query="SELECT throwIf(number = 1000000) FROM system.numbers" 2>&1 | grep -cF "$exception_pattern"
${CLICKHOUSE_CLIENT} --query="SELECT sum(x = 0) FROM (SELECT throwIf(number = 1000000) AS x FROM numbers(1000000))" 2>&1
