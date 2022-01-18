#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

default_exception_message="Value passed to 'throwIf' function is non zero"
custom_exception_message="Number equals 1000000"

${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000000) FROM system.numbers" 2>&1 | grep -cF "$default_exception_message"
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000000, '$custom_exception_message') FROM system.numbers" 2>&1 | grep -cF "$custom_exception_message"
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT sum(x = 0) FROM (SELECT throwIf(number = 1000000) AS x FROM numbers(1000000))" 2>&1
