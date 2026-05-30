#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

# Companion to 03164_s3_settings_for_queries_and_merges, but for the
# ReaderExecutor. The legacy test asserts S3 read efficiency through
# system.query_log ProfileEvents; the executor reads remote data through a
# different path, so here the same compact-part-on-S3 scan is asserted via the
# executor's own introspection in system.reader_executor_log.
#
# A cold scan after dropping the filesystem cache must read from the source and
# populate the cache. A warm re-scan of the same columns must then be served
# entirely from the filesystem cache, doing zero source I/O — that is the
# executor's "no over-read" guarantee. The window/seek knobs are set large so
# the cold scan coalesces instead of fanning out into many small source reads.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_s3_introspect"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_s3_introspect (c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
    ENGINE = MergeTree ORDER BY c1
    SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_cache'
"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_s3_introspect SELECT number, number, number, number, number FROM numbers(512 * 32 * 40)
"

COLD_ID="04302_re_cold_${CLICKHOUSE_DATABASE}"
WARM_ID="04302_re_warm_${CLICKHOUSE_DATABASE}"

# Executor on, with the read-ahead window and the seek-coalescing threshold set
# large so a full scan does not split into many small source reads.
RE_SETTINGS=(--use_reader_executor=1 --enable_reader_executor_log=1
    --reader_executor_window_size=16777216 --reader_executor_min_bytes_for_seek=16777216)

# Cold scan: cache dropped, so c2/c4 must be fetched from the source and pushed
# into the filesystem cache.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT "${RE_SETTINGS[@]}" --query_id "$COLD_ID" \
    --query "SELECT count() FROM t_re_s3_introspect WHERE NOT ignore(c2, c4) FORMAT Null"

# Warm scan: same columns, cache now populated, so it must be served from the
# filesystem cache with no source reads.
$CLICKHOUSE_CLIENT "${RE_SETTINGS[@]}" --query_id "$WARM_ID" \
    --query "SELECT count() FROM t_re_s3_introspect WHERE NOT ignore(c2, c4) FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS reader_executor_log"

# Cold: the executor ran and had to fetch column data from the source. (Some
# filesystem-cache bytes are expected too — s3_cache write-through from the
# INSERT and prefetch overlap leave part of the data cached even after the
# drop — so only the source read is asserted here.)
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        count() = 0
        OR sum(bytes_from_source) = 0,
        'cold scan: the executor did not read column data from the source')
    FROM system.reader_executor_log
    WHERE query_id = '$COLD_ID'
    FORMAT Null
"

# Warm: the same scan is now served entirely from the filesystem cache with zero
# source bytes — the executor does no redundant source I/O on a repeat read.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        count() = 0
        OR sum(bytes_from_filesystem_cache) = 0
        OR sum(bytes_from_source) != 0,
        'warm scan: expected cache-only reads, but the executor went to the source')
    FROM system.reader_executor_log
    WHERE query_id = '$WARM_ID'
    FORMAT Null
"
echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_s3_introspect"
