#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs the s3_cache storage policy.
# no-parallel: uses a server-wide failpoint that affects all cache writes.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A background-download cache write that fails with a non-ErrnoException must not
# leave an empty orphan cache file behind (would trip assertCacheCorrectness ->
# LOGICAL_ERROR "Expected file ... not to exist"). See issue #110532.

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_orphan"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_orphan (x UInt64, s String)
    ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_orphan SELECT number, toString(number) || '-pad-pad-pad' FROM numbers(100000)"
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP FILESYSTEM CACHE"

# Force a cache-write failure with a generic (non-Errno) Exception after the cache
# file was created. The read is expected to fail; we only care that no orphan file
# survives.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT cache_filesystem_failure_non_errno"
${CLICKHOUSE_CLIENT} --enable_filesystem_cache=1 --read_from_filesystem_cache_if_exists_otherwise_bypass_cache=0 \
    -q "SELECT sum(x), sum(length(s)) FROM t_orphan" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT cache_filesystem_failure_non_errno"

# The self-check runs inside getOrSet (debug/sanitizer builds). Before the fix this
# aborted the server; now it must pass and the read must return the correct result.
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP FILESYSTEM CACHE"
${CLICKHOUSE_CLIENT} --enable_filesystem_cache=1 -q "SELECT sum(x) FROM t_orphan"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_orphan"
