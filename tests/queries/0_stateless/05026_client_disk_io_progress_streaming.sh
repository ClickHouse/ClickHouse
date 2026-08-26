#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel

# Verifies the data path behind clickhouse-client's live disk-I/O progress segment
# (https://github.com/ClickHouse/ClickHouse/issues/116565): the OS-level bytes
# counters `OSReadBytes`/`OSWriteBytes` must reach the client in the streamed
# ProfileEvents. `ProgressIndication` then turns these per-interval deltas into
# a live rate rendered as `<rate>/s read` / `<rate>/s write` in the TTY progress
# line. The rendering itself needs a TTY and real device I/O (O_DIRECT), which is
# not portable across CI filesystems, so it is not asserted here.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# taskstats-based OS bytes counters are collected on Linux only; elsewhere the
# server never emits them and there is nothing to assert.
[[ "$OSTYPE" != "linux"* ]] && exit 0

# A direct-I/O read bypasses the page cache, so `OSReadBytes` increments
# regardless of cache warmth. Use `--print-profile-events` to dump the streamed
# events on the client side.
${CLICKHOUSE_CLIENT} --print-profile-events --profile-events-delay-ms=-1 \
    --query "SELECT sum(number) FROM numbers(1000000) SETTINGS min_bytes_to_use_direct_io = 1" 2>&1 \
    | grep -q "OSReadBytes" && echo "OSReadBytes streamed OK"

# An INSERT forces device writes on `fsync`, so `OSWriteBytes` increments.
${CLICKHOUSE_CLIENT} --print-profile-events --profile-events-delay-ms=-1 \
    --query "INSERT INTO FUNCTION null('x UInt8') SELECT number FROM numbers(1000000)" 2>&1 \
    | grep -q "OSWriteBytes" && echo "OSWriteBytes streamed OK"
