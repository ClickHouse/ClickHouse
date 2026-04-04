#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/74571
# S3 engine tables should expose hive partition columns when
# `use_hive_partitioning` is enabled at query time, even if it was
# disabled at CREATE TABLE time.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The test data at http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet
# is pre-populated by the test infrastructure (also used by 03203_hive_style_partitioning).

# -- Primary scenario (issue #74571): table created with use_hive_partitioning=0,
#    queried with use_hive_partitioning=1.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04077_s3_hive"
$CLICKHOUSE_CLIENT --use_hive_partitioning=0 -q "
    CREATE TABLE t_04077_s3_hive (first String, last String)
    ENGINE = S3('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet')
"

# With hive partitioning off at query time, the column should not be visible.
$CLICKHOUSE_CLIENT --use_hive_partitioning=0 -q "
    SELECT column0 FROM t_04077_s3_hive LIMIT 1
" 2>&1 | grep -cF "UNKNOWN_IDENTIFIER"

# With hive partitioning on at query time, the column should be dynamically
# exposed even though the table was created with the setting disabled.
$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
    SELECT column0 FROM t_04077_s3_hive LIMIT 1
"

# Filtering by the hive partition column should also work.
$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
    SELECT first FROM t_04077_s3_hive WHERE column0 = 'Elizabeth' LIMIT 1
"

# -- Secondary scenario: table created with use_hive_partitioning=1 works
#    normally when the setting is also on at query time.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04077_s3_hive_on"
$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
    CREATE TABLE t_04077_s3_hive_on (first String, last String)
    ENGINE = S3('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet')
"

$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
    SELECT column0 FROM t_04077_s3_hive_on LIMIT 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04077_s3_hive"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04077_s3_hive_on"
