#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: on Darwin StackTrace::resolveAddress keeps runtime addresses (AddressKind::Unsupported) and
# system.symbols reports absolute addresses of every loaded image, so no frame is stored as a file offset
# and max(address_end) is above all of them: neither column below can measure anything there.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# One mutable row per error code serves the whole server, so a code thrown by a test running in parallel
# answers the read below with its own trace. No query reaches the single `CANNOT_DLSYM` throw site in the
# server, which leaves this test as the only writer of that row.
$CLICKHOUSE_CLIENT -m -q "SELECT throwIf(true, 'file offsets', toInt16(300)) SETTINGS allow_custom_error_code_in_throwif = 1; -- { serverError CANNOT_DLSYM }"

# The trace columns of the system tables store frames inside the main binary as file offsets: a runtime
# address is only meaningful inside the process that produced it, and the binary is loaded at a different
# base on every start.
# `system.symbols` reports offsets into the binary, so its largest one bounds every frame the symbol table
# can resolve. A locally thrown exception captures deep ClickHouse frames, so requiring more than ten of
# them keeps a short, empty or missing trace from passing.
# Frames outside the main executable have to stay runtime addresses, which the second column asserts: an
# offset into a library is indistinguishable from a main executable offset once stored as a bare number,
# and the stack of a thrown exception ends in the C library.
# Keep the throw above shallow: a trace holds at most 32 frames and those C library ones are last.
$CLICKHOUSE_CLIENT -m -q "
SELECT
    countIf(x BETWEEN 1 AND (SELECT max(address_end) FROM system.symbols)) > 10,
    countIf(x > (SELECT max(address_end) FROM system.symbols)) > 0
FROM (
    SELECT arrayJoin(last_error_trace) AS x
    FROM system.errors
    WHERE code = 300 AND NOT remote)
SETTINGS allow_introspection_functions = 1;
"
