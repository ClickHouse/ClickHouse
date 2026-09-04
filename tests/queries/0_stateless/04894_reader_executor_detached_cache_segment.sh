#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings, no-distributed-cache, no-encrypted-storage, no-replicated-database
#
# Regression: the ReaderExecutor must serve a cache segment that FileCache returns in the
# DETACHED state as a read-through miss, instead of trying to become its downloader.
#
# `FileCache::getImpl` hands back a freshly-constructed DETACHED copy of any segment that is
# being evicted/removed (`isEvictingOrRemoved`). The executor's populate path treated every
# returned segment as a writable miss and called `getOrSetDownloader` on it, which aborts with
# "Cache file segment is in detached state, operation not allowed" (seen in CI as a background
# merge dying mid-INSERT under filesystem-cache eviction pressure). The read must instead fetch
# those bytes from the source and cache nothing for that segment.
#
# The `file_cache_simulate_evicting_segment` failpoint forces every cache lookup to see its
# segments as evicting, so the executor deterministically meets detached segments. The failpoint
# is process-global, hence no-parallel / no-replicated-database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_detached"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_detached (c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
    ENGINE = MergeTree ORDER BY c1
    SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_cache'
"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_detached SELECT number, number, number, number, number FROM numbers(512 * 32 * 40)
"

# Populate the filesystem cache with real segments for c2/c4 so the failpoint has something to
# report as evicting (an uncached hole comes back as a fresh EMPTY segment, not a detached one).
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT --use_reader_executor=1 --remote_filesystem_read_prefetch=0 \
    --query "SELECT count() FROM t_re_detached WHERE NOT ignore(c2, c4) FORMAT Null"

QID="04894_re_detached_${CLICKHOUSE_DATABASE}"

# Every cache lookup now returns born-DETACHED copies of the cached segments.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT file_cache_simulate_evicting_segment"

# The read must succeed, served read-through. Before the fix this aborts the server (debug/asan)
# or fails the query with a LOGICAL_ERROR (release).
#   expected: 655360
$CLICKHOUSE_CLIENT --use_reader_executor=1 --remote_filesystem_read_prefetch=0 --query_id "$QID" \
    --query "SELECT count() FROM t_re_detached WHERE NOT ignore(c2, c4)"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT file_cache_simulate_evicting_segment"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Step 2 confirmation: the executor actually met detached segments and served them read-through.
#   expected: 1
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['ReaderExecutorReadThroughDetachedSegments'] > 0
    FROM system.query_log
    WHERE query_id = '$QID' AND type = 'QueryFinish' AND current_database = currentDatabase()
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_detached"
