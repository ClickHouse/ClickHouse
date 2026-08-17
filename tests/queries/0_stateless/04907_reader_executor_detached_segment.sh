#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs an S3/minio-backed storage policy with a filesystem cache.
# no-parallel: arms a server-wide failpoint that affects every filesystem cache lookup.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint makes every segment the cache finds report itself as evicting or removed, so
# `getOrSet` hands the executor DETACHED placeholders. Such a segment holds no bytes and can never
# accept any, so it must be read from source rather than assigned a cache writer.

# `read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0` pins populate-on-miss: at 1 the
# executor's cache provider is read-only and never reaches the code path under test.
READ_SETTINGS=(
    --use_reader_executor=1
    --remote_filesystem_read_method=read
    --enable_filesystem_cache=1
    --read_from_filesystem_cache_if_exists_otherwise_bypass_cache=0
)

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_re_detached"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_re_detached (k UInt64, v String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS storage_policy = 's3_cache_04907', min_bytes_for_wide_part = 0"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO t_re_detached SELECT number, toString(number) FROM numbers(100000)
    SETTINGS enable_filesystem_cache_on_write_operations = 0"

# The failpoint is server-global and stays armed until an explicit disable, so it must be cleared even
# if the read below aborts the script. Otherwise every later filesystem cache lookup on this server
# receives DETACHED placeholders and stops serving cached bytes, masking the failure that happened here.
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT file_cache_simulate_evicting_segment" > /dev/null 2>&1 || true' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT file_cache_simulate_evicting_segment"
${CLICKHOUSE_CLIENT} "${READ_SETTINGS[@]}" \
    -q "SELECT count(), sum(k) FROM t_re_detached SETTINGS log_comment = 'reader_executor_detached'"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT file_cache_simulate_evicting_segment"

# The same read with the failpoint cleared, to keep the ordinary populate path covered.
${CLICKHOUSE_CLIENT} "${READ_SETTINGS[@]}" -q "SELECT count(), sum(k) FROM t_re_detached"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# `ReaderExecutorCacheGetRequests` is emitted only by the executor, and only on its cache path, so a
# non-zero count pins that the armed read went through `resolve` rather than some other read stage.
${CLICKHOUSE_CLIENT} -q "
    SELECT sumIf(ProfileEvents['ReaderExecutorCacheGetRequests'], log_comment = 'reader_executor_detached') > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= today() - 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_re_detached"
