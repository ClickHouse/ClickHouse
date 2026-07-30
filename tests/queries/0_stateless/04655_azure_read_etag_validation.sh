#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires azureBlobStorage
# Tag no-parallel: toggles a global failpoint

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT azure_read_inject_etag_mismatch" 2>/dev/null
}
trap on_exit EXIT

connection="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
container="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"
blob="04655_etag.csv"
parquet_blob="04655_etag.parquet"

${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION azureBlobStorage('$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SELECT number FROM numbers(100) SETTINGS azure_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION azureBlobStorage('$connection', '$container', '$parquet_blob', 'Parquet') SELECT number AS n FROM numbers(100) SETTINGS azure_truncate_on_insert = 1"

# Force every download to report a different ETag than the one captured at read setup, simulating a
# concurrent in-place overwrite. This exercises the whole path: the `s3_validate_etag_on_read` gate in
# `StorageObjectStorageSource`, the ETag carried into `ReadBufferFromAzureBlobStorage`, and the check
# performed on the download response.
# Every read below is a `SELECT count()`, which `optimize_count_from_files` can answer from the
# process-global row-count cache (`StorageObjectStorage::getSchemaCache`). That shortcut builds a
# `ConstChunkGenerator` instead of a read buffer, so no `ReadBufferFromAzureBlobStorage` is created
# and the ETag is never compared - the assertion would silently pass whatever the production code
# does. The `s3_validate_etag_on_read = 0` read below populates that cache for this very blob, so
# pin `use_cache_for_count_from_files = 0` on each validating query to keep it reading the object.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT azure_read_inject_etag_mismatch"

# Validation on (default): the injected mismatch must surface as `S3_OBJECT_CHANGED_DURING_READ`.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage('$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SETTINGS s3_validate_etag_on_read = 1, use_cache_for_count_from_files = 0" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1

# Validation off: the ETag is not propagated, so the same read succeeds.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage('$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SETTINGS s3_validate_etag_on_read = 0"

# The queries above only prove the direct `azureBlobStorage` path, where the ETag comes from the
# object's own metadata fetch. On the cluster path `StorageObjectStorageCluster::getTaskIteratorExtension`
# builds the iterator with `skip_object_metadata=true`, so a worker receives a placeholder `ObjectInfo`
# whose `etag` is empty and `StorageObjectStorageSource::createReader` has to refetch the metadata
# before reading - that refresh is what the production change widened from S3 to Azure. The explicit
# structure ('n UInt64') bypasses schema inference, so the failure must come from the distributed read
# and not from inference reading the blob on the coordinator first.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorageCluster('test_cluster_one_shard_three_replicas_localhost', '$connection', '$container', '$blob', 'CSV', 'auto', 'n UInt64') SETTINGS s3_validate_etag_on_read = 1, use_cache_for_count_from_files = 0" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1

# Same on the bucket-splitting variant, which wraps the placeholder iterator in
# ObjectIteratorSplitByBuckets and reads the object on the coordinator as well.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorageCluster('test_cluster_one_shard_three_replicas_localhost', '$connection', '$container', '$parquet_blob', 'Parquet', 'auto', 'n UInt64') SETTINGS s3_validate_etag_on_read = 1, cluster_table_function_split_granularity = 'bucket', use_cache_for_count_from_files = 0" 2>&1 \
    | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1
