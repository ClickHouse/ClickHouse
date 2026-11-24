#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-distributed-cache
# no-fasttest: needs an S3/minio-backed storage policy with a filesystem cache.
# no-parallel: arms a server-wide failpoint that affects every filesystem cache lookup.
# no-distributed-cache, no-random-settings: keep the read on the executor's cache path (else the assertion below fails).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint makes `getOrSet` hand back DETACHED placeholders, which must be read from source, not
# filled. bypass=0 pins populate-on-miss (the path under test); at 1 the provider is read-only.
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
    SETTINGS storage_policy = 's3_cache_04930', min_bytes_for_wide_part = 0"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO t_re_detached SELECT number, toString(number) FROM numbers(100000)
    SETTINGS enable_filesystem_cache_on_write_operations = 0"

# The failpoint is server-global; clear it even if the read aborts, or later cache reads on this
# server keep getting placeholders.
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT file_cache_simulate_evicting_segment" > /dev/null 2>&1 || true' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT file_cache_simulate_evicting_segment"
${CLICKHOUSE_CLIENT} "${READ_SETTINGS[@]}" \
    -q "SELECT count(), sum(k) FROM t_re_detached SETTINGS log_comment = 'reader_executor_detached'"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT file_cache_simulate_evicting_segment"

# The same read with the failpoint cleared, to keep the ordinary populate path covered.
${CLICKHOUSE_CLIENT} "${READ_SETTINGS[@]}" -q "SELECT count(), sum(k) FROM t_re_detached"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# A non-zero `ReaderExecutorCacheGetRequests` confirms the armed read went through the executor's cache path.
${CLICKHOUSE_CLIENT} -q "
    SELECT sumIf(ProfileEvents['ReaderExecutorCacheGetRequests'], log_comment = 'reader_executor_detached') > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= today() - 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_re_detached"
