#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Minio

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/91736
# Non-cluster table functions (url, s3) should not enable distributed_processing
# when evaluated on parallel replicas. The old code checked
# collaborate_with_initiator && hasClusterFunctionReadTaskCallback(), which was
# always true on replicas, causing "Distributed task iterator is not initialized".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

TABLE_NAME="${CLICKHOUSE_DATABASE}.test_04065"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE_NAME} (x UInt32) ENGINE = MergeTree() ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE_NAME} SELECT number FROM numbers(1000)"

# Insert test data into S3
$CLICKHOUSE_CLIENT <<EOF
INSERT INTO FUNCTION s3(
    'http://localhost:11111/test/$CLICKHOUSE_DATABASE/04065.tsv',
    'TSV',
    'x UInt32, y String') VALUES (1, 'a'), (2, 'b'), (3, 'c');
EOF

echo "--- url with parallel replicas ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 4;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_cluster_engines = true;

SELECT count() FROM url('http://localhost:8123/?query=SELECT+1', 'TSV', 'x UInt8');
EOF

echo "--- s3 with parallel replicas ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 4;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_cluster_engines = true;

SELECT count() FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/04065.tsv', 'TSV', 'x UInt32, y String');
EOF

echo "--- s3 with parallel replicas, verify data ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 4;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_cluster_engines = true;

SELECT * FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/04065.tsv', 'TSV', 'x UInt32, y String') ORDER BY x;
EOF

# Regression test for table functions like `paimonLocal` that do not have a
# `*Cluster` variant. With `parallel_replicas_for_cluster_engines = 1`, we must
# not try to rewrite `paimonLocal` to `paimonLocalCluster` on the worker (which
# would throw "Unknown table function") nor create `StorageObjectStorageCluster`
# on the initiator (which would distribute the query but each replica would read
# the same data redundantly).
# Use a per-database directory to avoid collisions between concurrent iterations
# in the flaky check (multiple parallel runs share `USER_FILES_PATH`, so the
# trailing `rm -rf` from one iteration could remove data while another iteration
# is still reading it).
PAIMON_DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_04065_data_minio"
mkdir -p "${PAIMON_DATA_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition/" "${PAIMON_DATA_DIR}/"

echo "--- paimonLocal (no Cluster variant) with parallel_replicas_for_cluster_engines ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 4;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_cluster_engines = true;
SET enable_time_time64_type = 1;

SELECT count() FROM paimonLocal('${PAIMON_DATA_DIR}/paimon_no_partition');
EOF

# This is the core bug reproduction: a MergeTree query with parallel replicas
# where url() appears in a subquery. On the replica, collaborate_with_initiator
# is true (for parallel replicas coordination), and the old code incorrectly set
# distributed_processing=true for url(). This caused the replica to send
# ReadTaskRequest packets to the initiator, which had no task_iterator for url().
#
# Note: parallel_replicas_for_non_replicated_merge_tree=1 is required so that
# parallel replicas actually distribute the MergeTree scan; without it the
# query runs locally on the initiator and the bug never manifests.
#
# Note: parallel_replicas_for_cluster_engines=0 is required so that the url()
# in the IN subquery is shipped to replicas as a plain url() (not auto-converted
# to urlCluster() on the initiator). Only then does the secondary query call
# TableFunctionURL::getStorage where the buggy distributed_processing=true
# branch existed.
#
# Note: parallel_replicas_local_plan=0 is required so that the initiator does
# not run a local plan that finishes first and cancels the remote replicas
# before they get to evaluate url(). Without this the secondary queries are
# cancelled with QUERY_WAS_CANCELLED_BY_CLIENT before the buggy code path is
# reached and the bug never surfaces on the master baseline binary.
echo "--- parallel replicas with url in subquery ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_cluster_engines = 0;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 0;

SELECT count() FROM ${TABLE_NAME}
WHERE x IN (SELECT toUInt32(x) FROM url('http://localhost:8123/?query=SELECT+1', 'TSV', 'x UInt8'));
EOF

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE_NAME}"
rm -rf "${PAIMON_DATA_DIR}"
