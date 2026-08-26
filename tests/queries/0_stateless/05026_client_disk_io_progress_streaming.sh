#!/usr/bin/env bash
# Tags: no-random-settings, no-darwin, no-parallel
# no-darwin: the OS-level bytes counters `OSReadBytes`/`OSWriteBytes` are collected from
#   Linux taskstats (/proc/thread-self/io) only; on other OSes they stay zero and are never
#   streamed, so the assertions below would not hold.

# Verifies the data path behind clickhouse-client's live disk-I/O progress segment
# (https://github.com/ClickHouse/ClickHouse/issues/116565): the OS-level bytes counters
# `OSReadBytes`/`OSWriteBytes` must reach the client in the streamed ProfileEvents.
# `ProgressIndication` then turns these per-interval deltas into a live rate rendered as
# `<rate>/s read` / `<rate>/s write` in the TTY progress line. The rendering itself needs a
# TTY and a multi-second device I/O burst, which is not portable across CI, so it is not
# asserted here; this pins the server->client streaming the feature rides on.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS disk_io_progress"
$CLICKHOUSE_CLIENT --query "CREATE TABLE disk_io_progress (x UInt64) ENGINE = MergeTree ORDER BY x AS SELECT number AS x FROM numbers(10000000)"

# A direct-I/O read over a real MergeTree table bypasses the page cache, so `OSReadBytes`
# increments regardless of cache warmth.
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 \
    --query "SELECT sum(x) FROM disk_io_progress FORMAT Null SETTINGS min_bytes_to_use_direct_io = 1, use_uncompressed_cache = 0" 2>&1 \
    | grep -o "OSReadBytes" | sort -u

# `fsync_after_insert` submits the writeback in the inserting thread's context, so
# `OSWriteBytes` increments for the query (buffered writeback would be attributed to kernel
# flusher threads instead).
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 --no-async-insert \
    --query "INSERT INTO disk_io_progress SELECT number AS x FROM numbers(10000000) SETTINGS fsync_after_insert = 1" 2>&1 \
    | grep -o "OSWriteBytes" | sort -u

$CLICKHOUSE_CLIENT --query "DROP TABLE disk_io_progress"
