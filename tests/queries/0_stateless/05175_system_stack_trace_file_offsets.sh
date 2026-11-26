#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: on Darwin StackTrace::resolveAddress keeps runtime addresses (AddressKind::Unsupported) and
# system.symbols reports absolute addresses of every loaded image, so no frame is stored as a file offset
# and max(address_end) is above all of them: neither column below can measure anything there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.stack_trace` stores frames inside the main binary as file offsets, which `system.symbols`
# bounds, and leaves every other frame as a runtime address, which is far above that bound. Threads
# waiting in the C library keep both kinds present in any snapshot of a running server.
query="
SELECT
    countIf(x BETWEEN 1 AND (SELECT max(address_end) FROM system.symbols)) > 10,
    countIf(x > (SELECT max(address_end) FROM system.symbols)) > 0
FROM (SELECT arrayJoin(trace) AS x FROM system.stack_trace)
SETTINGS allow_introspection_functions = 1"

# Signalling a thread can time out, leaving a snapshot with no deep trace to measure. Only that case is
# retried, so a trace stored the wrong way is reported at once instead of after every attempt.
for _ in {1..30}
do
    result=$($CLICKHOUSE_CLIENT -q "$query")
    [ "${result:0:1}" = "0" ] || break
done

echo "$result"
