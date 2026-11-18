#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, long
# Tag no-fasttest: needs the urlCluster/fileCluster/s3Cluster table functions and Minio
# Tag no-random-settings: pins max_streams_for_files_processing_in_cluster_functions
# Tag long: the clamp ceiling is 256 * number-of-cores, so each query materializes that many
#   sources; on high-core CI runners a single run approaches the 180s flaky-check limit.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Fast-fail HTTP settings keep every stream's connection attempt instant so the queries below
# stay quick even after num_streams is clamped to a large-but-bounded value.
FAST="http_max_tries = 1, http_connection_timeout = 1, http_receive_timeout = 1, http_send_timeout = 1"

# urlCluster, near-UINT64_MAX: used to throw std::length_error from pipes.reserve(num_streams).
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:9/', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 18446744073709551615, max_threads = 1, $FAST
" >/dev/null 2>&1

# urlCluster, merely huge: reserve(num_streams) succeeded but pipe.resize(max_num_streams) blew up.
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:9/', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 100000000, max_threads = 1, $FAST
" >/dev/null 2>&1

# fileCluster: the same setting reaches StorageFile/ReadFromFile. The local source count is clamped
# to the number of files, but pipe.resize(max_num_streams) under parallelize_output_from_storages
# used the raw (unclamped) value, so a huge value still drove the ResizeProcessor allocation.
mkdir -p "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/
echo "1" > "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file1.tsv

${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/file1.tsv', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 18446744073709551615, parallelize_output_from_storages = 1, max_threads = 1
" >/dev/null 2>&1

${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/file1.tsv', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 100000000, parallelize_output_from_storages = 1, max_threads = 1
" >/dev/null 2>&1

rm -f "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file1.tsv

# s3Cluster: the same setting reaches StorageObjectStorage/ReadFromObjectStorageStep. As with
# StorageFile, the local source count is clamped to estimatedKeysCount() but pipe.resize(max_num_streams)
# under parallelize_output_from_storages used the raw value, so a huge value drove the ResizeProcessor
# allocation through the object-storage cluster reader too.
S3_PATH="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/file1.tsv"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3('${S3_PATH}', 'TSV', 'a UInt8') VALUES (1)" >/dev/null 2>&1

${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM s3Cluster('test_cluster_two_shards_localhost', '${S3_PATH}', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 18446744073709551615, parallelize_output_from_storages = 1, max_threads = 1
" >/dev/null 2>&1

${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM s3Cluster('test_cluster_two_shards_localhost', '${S3_PATH}', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 100000000, parallelize_output_from_storages = 1, max_threads = 1
" >/dev/null 2>&1

# The server must still be alive and responsive after all queries.
${CLICKHOUSE_CLIENT} --query "SELECT 'OK'"
