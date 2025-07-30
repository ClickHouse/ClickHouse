#!/usr/bin/env bash
# Tags: no-fasttest
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Tag no-fasttest: Depends on Minio

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

$CLICKHOUSE_CLIENT <<EOF
SET enable_analyzer=1;
SET enable_parallel_replicas=1;
SET max_parallel_replicas=4;
SET cluster_for_parallel_replicas='test_cluster_two_shards';
SET parallel_replicas_for_cluster_engines=true;

CREATE OR REPLACE TABLE test_table (x UInt32, y UInt32, z UInt32) engine=MergeTree ORDER BY x AS SELECT * FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', 'TSV', 'x UInt32, y UInt32, z UInt32') LIMIT 100;
SELECT count() FROM test_table;
DROP TABLE IF EXISTS test_table;

SET parallel_replicas_for_cluster_engines=false;

CREATE OR REPLACE TABLE test_table (x UInt32, y UInt32, z UInt32) engine=MergeTree ORDER BY x AS SELECT * FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/03579.tsv', 'TSV', 'x UInt32, y UInt32, z UInt32') LIMIT 100;
SELECT count() FROM test_table;
DROP TABLE IF EXISTS test_table;
EOF
