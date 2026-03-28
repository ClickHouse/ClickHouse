#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Minio

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/91736
# Non-cluster table functions (url, s3) with parallel_replicas_for_cluster_engines
# should not throw "Distributed task iterator is not initialized".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

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
SET cluster_for_parallel_replicas = 'test_cluster_two_shards';
SET parallel_replicas_for_cluster_engines = true;

SELECT count() FROM url('http://localhost:8123/?query=SELECT+1', 'TSV', 'x UInt8');
EOF

echo "--- s3 with parallel replicas ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 4;
SET cluster_for_parallel_replicas = 'test_cluster_two_shards';
SET parallel_replicas_for_cluster_engines = true;

SELECT count() FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/04065.tsv', 'TSV', 'x UInt32, y String');
EOF

echo "--- s3 with parallel replicas, verify data ---"
$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 4;
SET cluster_for_parallel_replicas = 'test_cluster_two_shards';
SET parallel_replicas_for_cluster_engines = true;

SELECT * FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/04065.tsv', 'TSV', 'x UInt32, y String') ORDER BY x;
EOF
