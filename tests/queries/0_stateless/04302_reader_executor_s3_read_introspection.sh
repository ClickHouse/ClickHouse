#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

# Executor coverage of an S3 read through its own introspection in
# system.reader_executor_log. A cold scan after dropping the filesystem cache
# must read column data from the source and populate the cache; a warm re-scan
# of the same columns must then be served entirely from the filesystem cache,
# doing zero source I/O.
#
# remote_filesystem_read_prefetch=0 keeps cache population synchronous: without
# read-ahead the executor fills the cache on the read path itself, so by the
# time the cold query returns the data is cached and the warm read is
# deterministic (no race with a background prefetch writing the cache).

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

# Executor on, prefetch off so cache population is synchronous.
RE_SETTINGS=(--use_reader_executor=1 --enable_reader_executor_log=1 --remote_filesystem_read_prefetch=0)

# Cold scan: cache dropped, so c2/c4 are fetched from the source and synchronously
# written into the filesystem cache.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT "${RE_SETTINGS[@]}" --query_id "$COLD_ID" \
    --query "SELECT count() FROM t_re_s3_introspect WHERE NOT ignore(c2, c4) FORMAT Null"

# Warm scan: same columns, now served from the filesystem cache.
$CLICKHOUSE_CLIENT "${RE_SETTINGS[@]}" --query_id "$WARM_ID" \
    --query "SELECT count() FROM t_re_s3_introspect WHERE NOT ignore(c2, c4) FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS reader_executor_log"

# Cold: the executor ran, read from the source, and populated the cache.
#   expected: 1  1  1
$CLICKHOUSE_CLIENT --query "
    SELECT
        count() > 0,
        sum(bytes_from_source) > 0,
        sum(bytes_pushed_to_cache_sync + bytes_pushed_to_cache_async) > 0
    FROM system.reader_executor_log
    WHERE query_id = '$COLD_ID'
"

# Warm: served from the filesystem cache, zero source bytes.
#   expected: 1  1  0
$CLICKHOUSE_CLIENT --query "
    SELECT
        count() > 0,
        sum(bytes_from_filesystem_cache) > 0,
        sum(bytes_from_source)
    FROM system.reader_executor_log
    WHERE query_id = '$WARM_ID'
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_s3_introspect"
