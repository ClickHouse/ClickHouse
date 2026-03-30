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

# This is the core bug reproduction: a MergeTree query with parallel replicas
# where url() appears in a subquery. On the replica, collaborate_with_initiator
# is true (for parallel replicas coordination), and the old code incorrectly set
# distributed_processing=true for url(). This caused the replica to send
# ReadTaskRequest packets to the initiator, which had no task_iterator for url().
echo "--- parallel replicas with url in subquery ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT count() FROM ${TABLE_NAME}
WHERE x IN (SELECT toUInt32(x) FROM url('http://localhost:8123/?query=SELECT+1', 'TSV', 'x UInt8'));
EOF

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

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE_NAME}"
