#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT s3_read_inject_etag_mismatch" 2>/dev/null
}
trap on_exit EXIT

csv="http://localhost:11111/test/04339_etag_${CLICKHOUSE_DATABASE}.csv"
parq="http://localhost:11111/test/04339_etag_${CLICKHOUSE_DATABASE}.parquet"
auth="'test', 'testtest'"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('$csv', $auth, 'CSV', 'n UInt64') SELECT number FROM numbers(100) SETTINGS s3_truncate_on_insert=1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('$parq', $auth, 'Parquet') SELECT number AS n FROM numbers(100) SETTINGS s3_truncate_on_insert=1"

# Force every GET to report a different ETag than the one captured at read setup, simulating a
# concurrent in-place overwrite. This exercises the full path: the s3_validate_etag_on_read gate in
# StorageObjectStorageSource, the ETag carried into ReadBufferFromS3, and the check in sendRequest.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT s3_read_inject_etag_mismatch"

# Validation on (default): the injected mismatch must surface as S3_OBJECT_CHANGED_DURING_READ.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('$csv', $auth, 'CSV', 'n UInt64') SETTINGS s3_validate_etag_on_read = 1" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1

# Validation off: the ETag is not propagated, so the same read succeeds.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('$csv', $auth, 'CSV', 'n UInt64') SETTINGS s3_validate_etag_on_read = 0"

# s3Cluster with bucket splitting: the coordinator opens the object via createReadBuffer using a
# skip_object_metadata placeholder (empty etag). Validation must still fire there (createReadBuffer
# refetches the metadata to obtain the etag), so the read fails with S3_OBJECT_CHANGED_DURING_READ.
# The explicit structure ('n UInt64') bypasses schema inference, so the failure must come from the
# bucket-splitting read (ObjectIteratorSplitByBuckets), not from inference reading the object first.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', '$parq', $auth, 'Parquet', 'n UInt64') SETTINGS s3_validate_etag_on_read = 1, cluster_table_function_split_granularity = 'bucket'" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1
