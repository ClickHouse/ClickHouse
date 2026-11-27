#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database, memory-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A refresh period this far out yields a delay whose microsecond value overflows once the wait
# widens it to nanoseconds. The scheduler waits on the nearest deadline in the whole pool, so the
# huge delay only reaches that wait while it is the nearest one: true in clickhouse-local, but not
# in a server, whose own periodic tasks are always nearer.
log="${CLICKHOUSE_TMP}/04699_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "$log"*
# Route the sanitizer report to our own file (last log_path wins) so the oracle below is one
# build-invariant line rather than whatever the runner attaches as stderr.
o="log_path=$log"

ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    $CLICKHOUSE_LOCAL --query "
        CREATE MATERIALIZED VIEW mv_huge_delay
            REFRESH AFTER 20000000000 SECOND
            APPEND (x Int64) ENGINE = Memory AS SELECT 1 AS x;

        SELECT sleep(3) FORMAT Null;

        -- Must be exactly 1: with a second delayed task the wait is entered on the nearer
        -- deadline and the rest of this test proves nothing.
        SELECT count() FROM system.background_schedule_pool WHERE pool = 'schedule' AND delayed = 1;

        -- Parked, not fired: a delay that wrapped into a past deadline would refresh at once.
        SELECT count() FROM mv_huge_delay;
    "

cat "$log"* 2>/dev/null | grep -c 'signed integer overflow' || true
rm -f "$log"*
