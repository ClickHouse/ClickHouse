#!/usr/bin/env bash
# Tags: long, race
# Regression test for a data race / heap-use-after-free on the cached skp_idx.packed overlay reader.
#
# When several queries consult a packed part's skip-index overlay on its very first probe at the
# same time, getSkipIndicesPackedReader used to reseed (and free) the cached PackedFilesReader that
# another thread had already taken a raw pointer to and was reading -- a use-after-free that
# ThreadSanitizer reports as a data race / heap-use-after-free in the archive index. The fix makes
# the reader an immutable, shared, path-independent index so a reseed can no longer free a reader
# that is in use.
#
# The first-probe window is narrow (it only reads the archive header), so a single part rarely
# collides. PARTITION BY (k % 50) yields 50 freshly-unprobed packed parts, and one concurrent burst
# of index reads scans every part at once, so each query first-probes all parts together -- which
# makes the collision reliable in a single wave. Under a ThreadSanitizer build the old code reports
# the race; the fixed code and non-sanitizer builds just print "ok".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_packed_skip_race"

# min_bytes_for_full_part_storage='100G' forces packed part storage (the whole part lives in
# data.packed); packed_skip_index_max_bytes packs the minmax index into the inner skp_idx.packed
# archive. PARTITION BY (k % 50) yields 50 packed parts, each with an unprobed overlay.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_packed_skip_race (k UInt64, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY (k % 50) ORDER BY k
    SETTINGS min_bytes_for_full_part_storage = '100G', packed_skip_index_max_bytes = '1M'
"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_packed_skip_race SELECT number, number FROM numbers(50000)"

# One concurrent wave: each query scans all 50 parts, first-probing every part's overlay at once.
# The probe window is narrow, so a single wave reproduces the race most of the time rather than
# always -- which is fine, the TSan checks run this many times.
#
# The regression we guard against crashes the server (a heap-use-after-free that ThreadSanitizer
# aborts on), so it is detected by the queries below failing to connect, not by this benchmark's exit
# code. Ignore that exit code: under the flaky-check's randomized per-query settings an individual
# query can fail for reasons unrelated to the race, and a plain timeout under heavy CI load is also
# fine -- the wave has already raced the overlay by the time either happens.
timeout 60 $CLICKHOUSE_BENCHMARK -c 32 -i 32 <<< "SELECT count() FROM t_packed_skip_race WHERE v = 137" > /dev/null 2>&1 || true

$CLICKHOUSE_CLIENT -q "DROP TABLE t_packed_skip_race"

# If the server survived the concurrent reads (this query connects), the race did not fire.
$CLICKHOUSE_CLIENT -q "SELECT 'ok'"
