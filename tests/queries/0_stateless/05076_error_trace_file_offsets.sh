#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# One mutable row per error code serves the whole server and this code is thrown all over the suite, so
# the read below is constrained to a row refreshed after this point: an older event must not answer it.
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
