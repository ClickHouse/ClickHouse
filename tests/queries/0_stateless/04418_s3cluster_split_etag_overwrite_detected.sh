#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint

# Behavioral regression test for ETag propagation to s3Cluster workers (bucket-splitting path).
#
# The coordinator splits the object at generation G1 (reading its footer / capturing its ETag) and
# now propagates that ETag to the worker. We overwrite the object in place (-> G2) AFTER the split
# but BEFORE the worker reads it, using a pauseable failpoint as a deterministic barrier:
#
#   coordinator split (G1 captured)  ->  [worker paused here]  -> overwrite to G2 -> resume worker read
#
# With the fix the worker's GET is conditioned on G1 (If-Match), so the overwrite is reported as
# S3_OBJECT_CHANGED_DURING_READ. Without the fix the worker would re-HEAD after resuming, validate
# the new generation against itself, and silently return G2's (wrong) row count.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

fp="object_storage_source_pause_before_read"

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT $fp" 2>/dev/null
    wait 2>/dev/null
}
trap on_exit EXIT

parq="http://localhost:11111/test/04418_etag_${CLICKHOUSE_DATABASE}.parquet"
auth="'test', 'testtest'"

# Generation G1: 100 rows.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('$parq', $auth, 'Parquet') SELECT number AS n FROM numbers(100) SETTINGS s3_truncate_on_insert=1"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT $fp"

# Bucket-splitting s3Cluster read. Explicit structure bypasses schema inference. The worker pauses
# at the failpoint just before reading the split object.
out="${CLICKHOUSE_TMP}/04418_out_${CLICKHOUSE_DATABASE}.txt"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', '$parq', $auth, 'Parquet', 'n UInt64')
    SETTINGS s3_validate_etag_on_read = 1, cluster_table_function_split_granularity = 'bucket', enable_filesystem_cache = 0, use_cache_for_count_from_files = 0, s3_ignore_file_doesnt_exist = 0
" > "$out" 2>&1 &
query_pid=$!

# Wait until the worker has reached the failpoint (so the coordinator's split / ETag capture is done).
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT $fp PAUSE"

# Generation G2: overwrite the same key in place with a different size -> different ETag.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('$parq', $auth, 'Parquet') SELECT number AS n FROM numbers(50) SETTINGS s3_truncate_on_insert=1"

# Resume the worker. The PAUSEABLE_ONCE failpoint auto-disables on resume.
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT $fp"

wait "$query_pid" 2>/dev/null

# With the fix: the read fails with S3_OBJECT_CHANGED_DURING_READ. Without it: a silent count of 50.
grep -oF "S3_OBJECT_CHANGED_DURING_READ" "$out" | head -n 1
rm -f "$out"
