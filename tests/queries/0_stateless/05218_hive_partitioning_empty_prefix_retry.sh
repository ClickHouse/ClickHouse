#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/116888
# An empty listing must not be cached as a successful hive partitioning resolution:
# a table created over a prefix with no files yet must pick up the partition columns
# from the file paths once files appear.
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05218_hive"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE 05218_declared (id UInt64, key Nullable(String))
ENGINE = S3('$path/declared/key=*/*.parquet', 'test', 'testtest', 'Parquet');

CREATE TABLE 05218_undeclared (id UInt64)
ENGINE = S3('$path/undeclared/key=*/*.parquet', 'test', 'testtest', 'Parquet');

-- Both tables resolve the hive partitioning sample path over a currently-empty prefix.
SELECT count() FROM 05218_declared;
SELECT count() FROM 05218_undeclared;

INSERT INTO FUNCTION s3('$path/declared/key=A/data.parquet', 'test', 'testtest', 'Parquet') SELECT 1 AS id;
INSERT INTO FUNCTION s3('$path/undeclared/key=B/data.parquet', 'test', 'testtest', 'Parquet') SELECT 2 AS id;

-- A partition column declared in the schema must now be read from the file path,
-- and filtering on it must see the path values.
SELECT id, key FROM 05218_declared;
SELECT count() FROM 05218_declared WHERE key = 'A';

-- An undeclared partition column must be added to the table on the next resolution.
SELECT id, key FROM 05218_undeclared;

DROP TABLE 05218_declared;
DROP TABLE 05218_undeclared;
"
