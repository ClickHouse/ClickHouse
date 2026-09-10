#!/usr/bin/env bash
# Tags: no-parallel
# Reads a mutable `system.errors` row shared by every query using this error code.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Sequential execution prevents another test from replacing this query's error before the read.
# The timestamp guard also excludes entries left by earlier queries.
start_time=$($CLICKHOUSE_CLIENT -q "SELECT now()")

$CLICKHOUSE_CLIENT -m -q "SELECT throwIf(true, 'file offsets'); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }"

# The trace columns of the system tables store frames inside the main binary as file offsets: a runtime
# address is only meaningful inside the process that produced it, and the binary is loaded at a different
# base on every start.
# `system.symbols` reports offsets into the binary, so its largest one bounds every frame the symbol table
# can resolve. A locally thrown exception captures deep ClickHouse frames, so requiring more than ten of
# them keeps a short, empty or missing trace from passing.
# Frames outside the main executable have to stay runtime addresses, which the second column asserts: an
# offset into a library is indistinguishable from a main executable offset once stored as a bare number,
# and the stack of a thrown exception ends in the C library.
$CLICKHOUSE_CLIENT -m -q "
SELECT
    countIf(x BETWEEN 1 AND (SELECT max(address_end) FROM system.symbols)) > 10,
    countIf(x > (SELECT max(address_end) FROM system.symbols)) > 0
FROM (
    SELECT arrayJoin(last_error_trace) AS x
    FROM system.errors
    WHERE code = 395 AND NOT remote AND last_error_time >= '$start_time')
SETTINGS allow_introspection_functions = 1;
"
