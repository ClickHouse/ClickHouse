#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "SELECT 'TESTING THE FILE HIVE PARTITIONING'"


$CLICKHOUSE_LOCAL -q """
set use_hive_partitioning = 1;

SELECT *, column0 FROM file('$CURDIR/data_hive/partitioning/column0=Elizabeth/sample.parquet') LIMIT 10;

SELECT *, non_existing_column FROM file('$CURDIR/data_hive/partitioning/non_existing_column=Elizabeth/sample.parquet') LIMIT 10;
SELECT *, column0 FROM file('$CURDIR/data_hive/partitioning/column0=*/sample.parquet') WHERE column0 = 'Elizabeth' LIMIT 10;

SELECT number, date FROM file('$CURDIR/data_hive/partitioning/number=42/date=2020-01-01/sample.parquet') LIMIT 1;
SELECT array, float FROM file('$CURDIR/data_hive/partitioning/array=[1,2,3]/float=42.42/sample.parquet') LIMIT 1;
SELECT toTypeName(array), toTypeName(float) FROM file('$CURDIR/data_hive/partitioning/array=[1,2,3]/float=42.42/sample.parquet') LIMIT 1;
SELECT count(*) FROM file('$CURDIR/data_hive/partitioning/number=42/date=2020-01-01/sample.parquet') WHERE number = 42;
"""

$CLICKHOUSE_LOCAL -q """
set use_hive_partitioning = 1;

SELECT identifier FROM file('$CURDIR/data_hive/partitioning/identifier=*/email.csv') LIMIT 2;
SELECT a FROM file('$CURDIR/data_hive/partitioning/a=b/a=b/sample.parquet') LIMIT 1;
"""

$CLICKHOUSE_LOCAL -q """
set use_hive_partitioning = 1;

SELECT *, column0 FROM file('$CURDIR/data_hive/partitioning/column0=Elizabeth/column0=Elizabeth1/sample.parquet') LIMIT 10;
""" 2>&1 | grep -c "INCORRECT_DATA"

$CLICKHOUSE_LOCAL -q """
set use_hive_partitioning = 0;

SELECT *, non_existing_column FROM file('$CURDIR/data_hive/partitioning/non_existing_column=Elizabeth/sample.parquet') LIMIT 10;
""" 2>&1 | grep -c "UNKNOWN_IDENTIFIER"


$CLICKHOUSE_LOCAL -q "SELECT 'TESTING THE URL PARTITIONING'"


$CLICKHOUSE_LOCAL -q """
set use_hive_partitioning = 1;

SELECT *, column0 FROM url('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet') LIMIT 10;

SELECT *, non_existing_column FROM url('http://localhost:11111/test/hive_partitioning/non_existing_column=Elizabeth/sample.parquet') LIMIT 10;"""

$CLICKHOUSE_LOCAL -q """
set use_hive_partitioning = 0;

SELECT *, _column0 FROM url('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet') LIMIT 10;
""" 2>&1 | grep -c "UNKNOWN_IDENTIFIER"


$CLICKHOUSE_LOCAL -q "SELECT 'TESTING THE S3 PARTITIONING'"


$CLICKHOUSE_CLIENT -q """
set use_hive_partitioning = 1;

SELECT *, column0 FROM s3('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet') LIMIT 10;

SELECT *, non_existing_column FROM s3('http://localhost:11111/test/hive_partitioning/non_existing_column=Elizabeth/sample.parquet') LIMIT 10;
SELECT *, column0 FROM s3('http://localhost:11111/test/hive_partitioning/column0=*/sample.parquet') WHERE column0 = 'Elizabeth' LIMIT 10;
"""

$CLICKHOUSE_CLIENT -q """
set use_hive_partitioning = 0;

SELECT *, _column0 FROM s3('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet') LIMIT 10;
""" 2>&1 | grep -F -q "UNKNOWN_IDENTIFIER" && echo "OK" || echo "FAIL";

$CLICKHOUSE_LOCAL -q "SELECT 'TESTING THE S3CLUSTER PARTITIONING'"

$CLICKHOUSE_CLIENT -q """
set use_hive_partitioning = 1;

SELECT *, column0 FROM s3Cluster(test_cluster_one_shard_three_replicas_localhost, 'http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet') LIMIT 10;

SELECT *, column0 FROM s3Cluster(test_cluster_one_shard_three_replicas_localhost, 'http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet') WHERE column0 = 'Elizabeth' LIMIT 10;
"""
