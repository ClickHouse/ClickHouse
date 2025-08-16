#!/usr/bin/env bash
# Tags: no-fasttest
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Tag no-fasttest: Depends on Minio

set -eu

$CLICKHOUSE_CLIENT <<EOF
INSERT INTO FUNCTION s3(
    'http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv',
    'TSV',
    'x UInt32, y UInt32, z UInt32') VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9);
EOF

$CLICKHOUSE_CLIENT <<EOF
SELECT count() FROM s3(
    'http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv',
    'TSV');

SELECT * FROM s3(
    'http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv',
    'TSV');
EOF


for parallel_replicas_for_cluster_engines in 'false' 'true'
do
    echo "Testing with parallel_replicas_for_cluster_engines=$parallel_replicas_for_cluster_engines"

    $CLICKHOUSE_CLIENT <<EOF
    SET enable_analyzer=1;
    SET enable_parallel_replicas=1;
    SET max_parallel_replicas=4;
    SET cluster_for_parallel_replicas='test_cluster_two_shards';
    SET parallel_replicas_for_cluster_engines=${parallel_replicas_for_cluster_engines};

    CREATE OR REPLACE TABLE table (x UInt32, y UInt32, z UInt32)
        engine=MergeTree
        ORDER BY x
        AS SELECT *
            FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', NOSIGN, 'TSV')
            LIMIT 100;
    SELECT 'count from table', count() FROM table;
    DROP TABLE IF EXISTS table;

    CREATE OR REPLACE TABLE table_s3Cluster (x UInt32, y UInt32, z UInt32)
        engine=MergeTree
        ORDER BY x
        AS SELECT *
            FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', 'TSV')
            LIMIT 100;
    SELECT 'count from table_s3Cluster', count() FROM table_s3Cluster;
    DROP TABLE IF EXISTS table_s3Cluster;
EOF

done

exit 0

replicated_db="${CLICKHOUSE_DATABASE}_replicated"
cleanup_replicated_database() {
    set +e
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${replicated_db};"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${replicated_db}_second;"
}
trap cleanup_replicated_database EXIT

$CLICKHOUSE_CLIENT <<EOF
CREATE DATABASE IF NOT EXISTS ${replicated_db} ENGINE = Replicated('/clickhouse/{database}/replicated_db', 'shard_1');
CREATE DATABASE IF NOT EXISTS ${replicated_db}_second ENGINE = Replicated('/clickhouse/{database}/replicated_db', 'shard_2');
EOF

$CLICKHOUSE_CLIENT <<EOF
CREATE OR REPLACE TABLE ${replicated_db}.table_s30 (x UInt32, y UInt32, z UInt32)
    engine=Memory
    AS SELECT *
        FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', 'TSV', 'x UInt32, y UInt32, z UInt32')
        LIMIT 100;
SELECT count() FROM ${replicated_db}.table_s30;
EOF

$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer=1;
SET enable_parallel_replicas=1;
SET max_parallel_replicas=4;
SET cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_cluster_engines=1;
CREATE OR REPLACE TABLE ${replicated_db}.table_s3 (x UInt32, y UInt32, z UInt32)
    engine=Memory
    AS SELECT *
        FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', 'TSV', 'x UInt32, y UInt32, z UInt32')
        LIMIT 100;
SELECT count() FROM ${replicated_db}.table_s3;
EOF

$CLICKHOUSE_CLIENT <<EOF
CREATE OR REPLACE TABLE ${replicated_db}.table_s3Cluster (x UInt32, y UInt32, z UInt32)
    engine=Memory
    AS SELECT *
        FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', 'TSV', 'x UInt32, y UInt32, z UInt32')
        LIMIT 100;
SELECT count() FROM ${replicated_db}.table_s3Cluster;
EOF
