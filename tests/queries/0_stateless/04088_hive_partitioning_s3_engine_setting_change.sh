#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/74571
# S3 engine tables should expose hive partition columns as *static* virtual
# columns, regardless of whether `use_hive_partitioning` was enabled at
# CREATE TABLE or at query time.  The session setting still controls whether
# hive columns participate in *inferred-schema enrichment*, but column
# existence and value population are fixed at CREATE TABLE.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The test data at http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet
# is pre-populated by the test infrastructure (also used by 03203_hive_style_partitioning).

# -- Primary scenario (issue #74571): table created with use_hive_partitioning=0,
#    queried with use_hive_partitioning=1 — column is exposed and populated.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04077_s3_hive"
$CLICKHOUSE_CLIENT --use_hive_partitioning=0 -q "
    CREATE TABLE t_04077_s3_hive (first String, last String)
    ENGINE = S3('http://localhost:11111/test/hive_partitioning/column0=Elizabeth/sample.parquet')
"

# The hive column is now a static virtual and is visible regardless of the query-time setting.
$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
    SELECT column0 FROM t_04077_s3_hive LIMIT 1
"

# Filtering by the hive partition column should also work.
$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
    SELECT first FROM t_04077_s3_hive WHERE column0 = 'Elizabeth' LIMIT 1
"

# -- Regression for the audit-time finding: with `use_hive_partitioning=0` at *query* time the
#    chunk still has to contain the hive column, otherwise downstream sees a Block-structure
#    mismatch on the static virtual.  Query the virtual column directly to force it into the
#    pipeline; we just need the query to succeed with a populated value.
$CLICKHOUSE_CLIENT --use_hive_partitioning=0 -q "
    SELECT column0 FROM t_04077_s3_hive LIMIT 1
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
