#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses urlCluster/s3Cluster over the HTTP port and Minio, and several test clusters

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/91736
#
# A cluster table function (urlCluster, s3Cluster, fileCluster) nested inside an
# outer distribution (clusterAllReplicas / parallel replicas) used to abort the
# server with "Distributed task iterator is not initialized" (LOGICAL_ERROR in
# RemoteQueryExecutor::processReadTaskRequest).
#
# The outer clusterAllReplicas ships `SELECT ... FROM urlCluster(...)` to its
# replicas as secondary queries. On a replica the inner urlCluster saw
# query_kind == SECONDARY_QUERY and enabled distributed_processing, so it sent a
# ReadTaskRequest back to the outer initiator - a plain StorageDistributed that
# had set up no cluster-function task iterator for it, hence the logical error.
#
# PR #100146 fixed the auto-converted non-cluster path (url/s3 promoted to cluster
# mode by parallel_replicas_for_cluster_engines); this covers the explicit
# *Cluster table functions nested under another distribution. The worker now keys
# distributed_processing on an explicit initiator signal
# (ClientInfo::is_cluster_function_distributed_read) instead of just
# collaborate_with_initiator, so a legitimate INSERT INTO distributed SELECT FROM
# *Cluster (distributed_depth == 1, iterator present) keeps reading each task once.
#
# The exact row count depends on replica selection (e.g. prefer_localhost_replica)
# so the queries are wrapped in `count() >= 1` - what matters is that they run to
# completion and the server does not abort.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

echo "--- clusterAllReplicas(urlCluster(SELECT 1)) ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() >= 1
FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    urlCluster('test_cluster_two_shards', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8'))
"

echo "--- clusterAllReplicas(urlCluster(...)) with WHERE ---"
# The WHERE filter must reference the inner column directly. clusterAllReplicas ships
# the inner SELECT to its replicas without the outer alias, so a `t.`-qualified filter
# is unresolvable there under the old analyzer (Missing columns: 't.x').
$CLICKHOUSE_CLIENT --query "
SELECT count() >= 1
FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    urlCluster('test_cluster_two_shards', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8'))
WHERE x = 1
"

# fileCluster nested inside clusterAllReplicas. Reads a file from user_files.
DATA_FILE="${CLICKHOUSE_DATABASE}_03850.tsv"
DATA_PATH="${USER_FILES_PATH}/${DATA_FILE}"
printf '1\n2\n3\n' > "${DATA_PATH}"

echo "--- clusterAllReplicas(fileCluster(...)) ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() >= 1
FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    fileCluster('test_cluster_two_shards', '${DATA_FILE}', 'TSV', 'x UInt8'))
"

# s3Cluster nested inside clusterAllReplicas. The object-storage *Cluster stack
# (s3Cluster/icebergCluster/...) goes through a different iterator path
# (StorageObjectStorageSource::createFileIterator -> ReadTaskIterator) wired in
# TableFunctionObjectStorageCluster, so it must be covered separately from the
# url/file path above. Writes a small object to the localhost Minio test bucket first.
# prefer_localhost_replica=0 forces the outer clusterAllReplicas to ship the inner
# query to remote replicas as a real secondary query (otherwise it runs locally and
# the broken nested path is never exercised - the pre-fix server aborts here with
# "Distributed task iterator is not initialized").
S3_PATH="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_03850.tsv"
$CLICKHOUSE_CLIENT --query "
INSERT INTO FUNCTION s3('${S3_PATH}', 'TSV', 'x UInt8')
SETTINGS s3_truncate_on_insert = 1
VALUES (1), (2), (3)
"

echo "--- clusterAllReplicas(s3Cluster(...)) ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() >= 1
FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    s3Cluster('test_cluster_two_shards', '${S3_PATH}', 'TSV', 'x UInt8'))
SETTINGS prefer_localhost_replica = 0
"

# The legitimate top-level cluster table function must still distribute correctly
# (the fix must not disable distributed_processing for the real, non-nested case).
echo "--- top-level urlCluster still works ---"
$CLICKHOUSE_CLIENT --query "
SELECT count()
FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')
"

# INSERT INTO distributed SELECT FROM urlCluster(...) with parallel_distributed_insert_select=2
# is a legitimate cluster-function distribution at distributed_depth == 1: the initiator DOES
# install a read-task iterator, so each globbed task must be read exactly once. The fix must
# keep this working (a depth-based guard would wrongly disable it and each replica would re-read
# the whole glob, duplicating rows). The source URL serves the three values {1,2,3} as separate
# tasks; the destination is a single-shard cluster, so exactly 3 rows must be inserted (the same
# shape as the existing 01099_parallel_distributed_insert_select coverage).
echo "--- INSERT INTO distributed SELECT FROM urlCluster reads each task once ---"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS local_03850 SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS dist_03850 SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE local_03850 (s String) ENGINE = MergeTree ORDER BY s"
$CLICKHOUSE_CLIENT --query "CREATE TABLE dist_03850 AS local_03850 ENGINE = Distributed('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), local_03850, rand())"
$CLICKHOUSE_CLIENT --query "
INSERT INTO dist_03850
SELECT * FROM urlCluster('test_cluster_two_shards', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=select+{1,2,3}+format+TSV', 'TSV', 's String')
SETTINGS parallel_distributed_insert_select = 2
"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM dist_03850"
$CLICKHOUSE_CLIENT --query "DROP TABLE local_03850 SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE dist_03850 SYNC"

# The server must still be alive after the queries above.
echo "--- server alive ---"
$CLICKHOUSE_CLIENT --query "SELECT 1"

rm -f "${DATA_PATH}"
