#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# A cache hit reported by DiskCacheHandle::status() must still be readable by
# get() even when SYSTEM DROP FILESYSTEM CACHE runs in between. The handle holds
# a FileSegmentsHolder that keeps the segment non-releasable, and DROP — like
# LRU eviction — skips non-releasable segments (FileCache LockedKey::
# removeAllFileSegments checks releasable() == isSharedPtrUnique). So the held
# hit must survive the drop and get() must honor it.
#
# The reader_executor_pause_after_cache_status failpoint pauses exactly in the
# status()->get() window, making the drop deterministic. If the segment were
# lost there, the executor's covers() guard in readPhysicalWindow throws
# LOGICAL_ERROR; this test asserts it does not and the scan returns correct data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="reader_executor_pause_after_cache_status"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_status_get"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_status_get (key UInt32, value String)
    ENGINE = MergeTree() ORDER BY key
    SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0
"
# Incompressible data spanning several 5 MiB cache segments.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_status_get SELECT number, randomPrintableASCII(100) FROM numbers(300000)
"

# Clean slate, then warm the cache so the scan below finds hits in status().
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
EXPECTED=$($CLICKHOUSE_CLIENT --use_reader_executor=1 --query "
    SELECT count(), sum(cityHash64(value)) FROM t_re_status_get
")

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

# Scan through the executor; pauses after the first cache hit is classified by
# status(), before get() reads it.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --query "
    SELECT count(), sum(cityHash64(value)) FROM t_re_status_get
" > "$CLICKHOUSE_TMP/re_status_get.txt" 2>&1 &
SELECT_PID=$!

if timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    # Drop the cache inside the status()->get() window. The hit segment this
    # handle holds is non-releasable, so it must survive and get() must honor it.
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
else
    echo "FAIL: scan did not reach the pause failpoint"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait "$SELECT_PID"

ACTUAL=$(cat "$CLICKHOUSE_TMP/re_status_get.txt")
if [ "$ACTUAL" == "$EXPECTED" ]; then
    echo "status hit honored across drop: OK"
else
    echo "FAIL: result mismatch or executor error: got [$ACTUAL] expected [$EXPECTED]"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_status_get"
