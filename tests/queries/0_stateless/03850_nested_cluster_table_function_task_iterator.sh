#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses urlCluster/fileCluster/s3Cluster over the HTTP/Minio ports and several test clusters

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/91736
#
# A cluster table function (urlCluster, fileCluster, ...) executed on a worker sends a read-task
# request back to the initiator. Serving it needs a task iterator, which the initiator installs
# only for the legitimate dispatch paths. When the function is reached through an outer
# distribution the iterator is absent and processReadTaskRequest used to hit a LOGICAL_ERROR that
# aborts the server. The request now carries enough state to tell the two unsupported shapes apart,
# so each is rejected with its own BAD_ARGUMENTS message (a normal query error, the server stays
# alive), while the legitimate top-level and INSERT-SELECT paths keep distributing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

DATA_FILE="${CLICKHOUSE_DATABASE}_03850.tsv"
DATA_PATH="${USER_FILES_PATH}/${DATA_FILE}"
printf '1\n2\n3\n' > "${DATA_PATH}"

# Shape 1: a cluster table function nested directly inside an outer clusterAllReplicas. On the
# worker the executor has no extension at all, so the request is rejected as a nested cluster
# function. prefer_localhost_replica=0 forces the outer query to ship the inner one to a remote
# replica as a real secondary query, so the rejection gate is reached.
echo "--- shape 1: nested cluster table function (urlCluster) ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    urlCluster('test_cluster_two_shards', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8'))
SETTINGS prefer_localhost_replica = 0
" 2>&1 | grep -o -m1 "cannot be nested inside another distributed query"

echo "--- shape 1: nested cluster table function (fileCluster) ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    fileCluster('test_cluster_two_shards', '${DATA_FILE}', 'TSV', 'x UInt8'))
SETTINGS prefer_localhost_replica = 0
" 2>&1 | grep -o -m1 "cannot be nested inside another distributed query"

# Shape 1, object storage: an explicit s3Cluster nested inside the outer clusterAllReplicas. The
# object-storage *Cluster stack reaches the read task through different code than url/file
# (StorageObjectStorageSource), so it is exercised separately. It must also be rejected, not
# silently read once per outer replica (which would multiply the row count).
S3_OBJECT="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_03850.tsv"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('${S3_OBJECT}', 'TSV', 'x UInt8') SELECT 1 SETTINGS s3_truncate_on_insert = 1"

echo "--- shape 1: nested cluster table function (s3Cluster) ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM clusterAllReplicas(
    'test_cluster_one_shard_three_replicas_localhost',
    s3Cluster('test_cluster_two_shards', '${S3_OBJECT}', 'TSV', 'x UInt8'))
SETTINGS prefer_localhost_replica = 0
" 2>&1 | grep -o -m1 "cannot be nested inside another distributed query"

# Shape 2: a cluster table function reached inside a query that itself runs with parallel replicas.
# The initiator builds a parallel-replicas executor (it has a parallel-reading coordinator but no
# task iterator), and the inner urlCluster on a replica sends a read-task request to it. This is a
# different, also unsupported, configuration and gets its own message.
# enable_analyzer=1 is required: only the analyzer ships the cluster-function subquery to the
# replicas as a secondary query that hits the gate (the old analyzer evaluates it locally and the
# request is never sent). parallel_replicas_for_non_replicated_merge_tree=1 makes parallel replicas
# actually distribute the MergeTree scan; parallel_replicas_local_plan=0 keeps the initiator from
# finishing locally and cancelling the replicas before they evaluate the inner function.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mt_03850 SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt_03850 (x UInt32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "INSERT INTO mt_03850 SELECT number FROM numbers(100)"

echo "--- shape 2: cluster table function under parallel replicas ---"
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM mt_03850
WHERE x IN (SELECT toUInt32(x) FROM urlCluster('test_cluster_two_shards', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8'))
SETTINGS enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
    parallel_replicas_for_cluster_engines = 0
" 2>&1 | grep -o -m1 "cannot use distributed processing inside a query that runs with parallel replicas"

$CLICKHOUSE_CLIENT --query "DROP TABLE mt_03850 SYNC"

# The legitimate top-level cluster table function is not nested: the initiator installs the
# read-task iterator, so it must still distribute correctly.
echo "--- top-level urlCluster still works ---"
$CLICKHOUSE_CLIENT --query "
SELECT count()
FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')
"

# INSERT INTO distributed SELECT FROM urlCluster(...) with parallel_distributed_insert_select=2
# is a legitimate cluster-function distribution: the initiator installs a read-task iterator, so
# each globbed task must be read exactly once (the rejection must not fire here). The source serves
# {1,2,3} as three tasks into a single-shard destination, so exactly 3 rows are inserted (the same
# shape as 01099_parallel_distributed_insert_select).
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

# The server must still be alive after the rejected queries above (BAD_ARGUMENTS does not abort).
echo "--- server alive ---"
$CLICKHOUSE_CLIENT --query "SELECT 1"

rm -f "${DATA_PATH}"
