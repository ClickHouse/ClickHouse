#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# End-to-end check of the ReaderExecutor in-flight segment pin on real
# components (S3-backed `s3_cache` disk, real FileCache, real executor).
#
# While a sequential scan is paused mid-window with its in-flight FileCache
# segment pinned, a concurrent `SYSTEM DROP FILESYSTEM CACHE` must NOT evict
# that segment (it is non-releasable while pinned), so the cache is not empty
# during the pause. Once the scan finishes and the pin is released, a drop
# clears the cache. The scan must return correct results despite the mid-scan
# drop. The pause is injected via the `reader_executor_pause_after_window`
# failpoint (fires only when a segment is actually pinned).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="reader_executor_pause_after_window"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_pin"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_pin (key UInt32, value String)
    ENGINE = MergeTree() ORDER BY key
    SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0
"
# Incompressible column data so the value column spans several 5 MiB cache
# segments (and read windows land mid-segment, leaving a PARTIALLY_DOWNLOADED
# segment for the executor to pin). Compressible data would collapse into a
# single whole segment and never produce a partial one.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_pin SELECT number, randomPrintableASCII(100) FROM numbers(300000)
"

EXPECTED=$($CLICKHOUSE_CLIENT --query "SELECT count(), sum(cityHash64(value)) FROM t_re_pin")

# Clean slate so system.filesystem_cache reflects only this scan.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

# Sequential scan through the executor; pauses after the first window that
# pins its in-flight segment.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --query "
    SELECT count(), sum(cityHash64(value)) FROM t_re_pin
" > "$CLICKHOUSE_TMP/re_pin_result.txt" 2>&1 &
SELECT_PID=$!

# Block until the scan is actually paused at the failpoint (pin held).
if timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    # While paused the in-flight segment is pinned, so the drop must leave it.
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
    SURVIVED=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache")
    if [ "$SURVIVED" -gt 0 ]; then
        echo "pinned segment survived drop: OK"
    else
        echo "FAIL: nothing survived the mid-scan drop, got $SURVIVED"
    fi
else
    echo "FAIL: scan did not reach the pause failpoint"
fi

# Resume the scan and let it finish.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait "$SELECT_PID"

# The pin is released now, so the cache is fully releasable again. The scan's
# server-side teardown (executor destruction, prefetch-worker drain, pin
# release) can lag the client's exit that `wait` observed, so a single drop may
# race segments that are momentarily still held. Retry the drop until the cache
# empties; a genuine leak never empties and still fails this bounded loop.
AFTER=1
for _ in $(seq 1 100); do
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
    AFTER=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.filesystem_cache")
    [ "$AFTER" -eq 0 ] && break
done
if [ "$AFTER" -eq 0 ]; then
    echo "cache empty after release: OK"
else
    echo "FAIL: $AFTER segments remained after the scan finished"
fi

ACTUAL=$(cat "$CLICKHOUSE_TMP/re_pin_result.txt")
if [ "$ACTUAL" == "$EXPECTED" ]; then
    echo "result correct: OK"
else
    echo "FAIL: result mismatch: got [$ACTUAL] expected [$EXPECTED]"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_pin"
