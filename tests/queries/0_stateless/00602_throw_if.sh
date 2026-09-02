#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


default_exception_message="Value passed to 'throwIf' function is non-zero"
custom_exception_message="Number equals 1000"

${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000) FROM system.numbers" 2>&1 \
    | grep -cF "$default_exception_message"

${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000, '$custom_exception_message') FROM system.numbers" 2>&1 \
    | grep -v '^(query: ' | grep -cF "$custom_exception_message"


# Custom error code arguments are not enabled via configuration.
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000, '$custom_exception_message', 1) FROM system.numbers" 2>&1 \
    | grep -v '^(query: ' | grep -c "Number of arguments for function throwIf doesn't match: passed 3, should be 1 or 2"

# Custom error code argument enabled but using the wrong type.
${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000, '$custom_exception_message', 1) FROM system.numbers SETTINGS allow_custom_error_code_in_throwif=true" 2>&1 \
    | grep -v '^(query: ' | grep -c "Third argument of function throwIf must be Int8, Int16 or Int32 (passed: UInt8)"


# Normal error code + some weird ones.
# Internal error codes use the upper half of 32-bit int.
custom_error_codes=(
    "42"
    "0"       # OK
    "101"     # UNEXPECTED_PACKET_FROM_CLIENT (interpreted by client)
    "102"     # UNEXPECTED_PACKET_FROM_SERVER (interpreted by client)
    "1001"    # STD_EXCEPTION
    "1002"    # UNKNOWN_EXCEPTION
    "999999"  # Unused error code.
    "-1")     # Also unused. Weird but we should allow throwing negative errors.

for ec in "${custom_error_codes[@]}"
do
    ${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT throwIf(number = 1000, '$custom_exception_message', toInt32($ec)) FROM system.numbers SETTINGS allow_custom_error_code_in_throwif=true" 2>&1 \
        | grep -v '^(query: ' | grep -c "Code: $ec.*$custom_exception_message"
done


${CLICKHOUSE_CLIENT} --server_logs_file /dev/null --query="SELECT sum(x = 0) FROM (SELECT throwIf(number = 1000) AS x FROM numbers(1000))" 2>&1
