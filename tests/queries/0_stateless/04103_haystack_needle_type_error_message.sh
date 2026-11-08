#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check that `locate` reports the correct type in error messages,
# under both needle-haystack and haystack-needle argument orders.
#
# Previously, `SELECT locate('hi', 42)` reported
#     Illegal type String of argument of function locate
# instead of
#     Illegal type UInt8 of argument of function locate

extract_bad_type() {
    grep -m1 -o 'Illegal type .* of argument' | sed 's/ of argument$//'
}

# Default order: locate(needle, haystack[, start_pos])
$CLICKHOUSE_CLIENT --query "SET function_locate_has_mysql_compatible_argument_order = 1; SELECT locate('hi', 42::UInt8)" 2>&1 | extract_bad_type
$CLICKHOUSE_CLIENT --query "SET function_locate_has_mysql_compatible_argument_order = 1; SELECT locate(100::UInt8, 'hello')" 2>&1 | extract_bad_type
$CLICKHOUSE_CLIENT --query "SET function_locate_has_mysql_compatible_argument_order = 1; SELECT locate('hi', 33::UInt8, 1)" 2>&1 | extract_bad_type

# Classic order: locate(haystack, needle[, start_pos])
$CLICKHOUSE_CLIENT --query "SET function_locate_has_mysql_compatible_argument_order = 0; SELECT locate(42::UInt8, 'hello')" 2>&1 | extract_bad_type
$CLICKHOUSE_CLIENT --query "SET function_locate_has_mysql_compatible_argument_order = 0; SELECT locate('hi', 42::UInt8)" 2>&1 | extract_bad_type
