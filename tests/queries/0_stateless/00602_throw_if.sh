#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

default_exception_message="Value passed to 'throwIf' function is non-zero"
custom_exception_message="Number equals 1000000"
custom_error_code="42"

${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000000) FROM system.numbers" 2>&1 | grep -cF "$default_exception_message"
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000000, '$custom_exception_message') FROM system.numbers" 2>&1 | grep -v '^(query: ' | grep -cF "$custom_exception_message"
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000000, '$custom_exception_message', toInt32($custom_error_code)) FROM system.numbers SETTINGS allow_custom_error_code_in_throwif=true" 2>&1 | grep -v '^(query: ' | grep -c "Code: $custom_error_code.*$custom_exception_message"
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT sum(x = 0) FROM (SELECT throwIf(number = 1000000) AS x FROM numbers(1000000))" 2>&1
