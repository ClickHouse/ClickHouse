#!/usr/bin/env bash
# Hive-ecosystem writers stage in-progress output and place marker objects under path segments
# starting with '_' or '.' (Spark/Hadoop FileOutputCommitter: `_temporary/.../part-*.parquet`,
# Spark dynamic partition overwrite: `.spark-staging-<jobId>/...`, plus `_SUCCESS`). Every other
# reader of Hive-layout data skips such paths. Tables with partition_strategy='hive' must skip
# them unconditionally; plain glob paths skip them under the s3_skip_hidden_files setting, and
# segments within the non-glob prefix of the path are exempt.
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05236"

$CLICKHOUSE_CLIENT -q "
-- One committed file, two staged files and a marker under the same table root.
INSERT INTO FUNCTION s3('$path/t/dt=2026-01-01/a.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(1) AS id;
INSERT INTO FUNCTION s3('$path/t/_temporary/0/_temporary/attempt_x/dt=2026-01-01/b.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(2) AS id;
INSERT INTO FUNCTION s3('$path/t/.spark-staging-y/dt=2026-01-01/c.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(3) AS id;
INSERT INTO FUNCTION s3('$path/t/_SUCCESS', 'test', 'testtest', 'CSV') SELECT 1;

-- The hive partition strategy skips hidden paths unconditionally.
CREATE TABLE 05236_hidden (id Int64, dt String)
ENGINE = S3('$path/t', 'test', 'testtest', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY dt;

SELECT 'hive count:', count() FROM 05236_hidden;
SELECT 'hive filtered count:', count() FROM 05236_hidden WHERE dt = '2026-01-01';
SELECT 'hive rows:', id, dt FROM 05236_hidden ORDER BY id;

DROP TABLE 05236_hidden;

-- A plain glob keeps the old behavior by default and skips hidden paths under the setting.
SELECT 'plain glob default:', count() FROM s3('$path/t/**.parquet', 'test', 'testtest', 'Parquet');
SELECT 'plain glob skip hidden:', count() FROM s3('$path/t/**.parquet', 'test', 'testtest', 'Parquet') SETTINGS s3_skip_hidden_files = 1;

-- Hidden segments within the non-glob prefix of the path are exempt: only segments the glob
-- listing discovers below the prefix are checked.
INSERT INTO FUNCTION s3('$path/_root/dt=2026-01-01/d.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(4) AS id;
SELECT 'hidden prefix exempt:', count() FROM s3('$path/_root/*/*.parquet', 'test', 'testtest', 'Parquet') SETTINGS s3_skip_hidden_files = 1;

-- Schema inference samples the listing in order, and '_temporary' sorts before 'dt=...':
-- without the filter it infers the schema of a staged file.
INSERT INTO FUNCTION s3('$path/t2/_temporary/0/staged.parquet', 'test', 'testtest', 'Parquet') SELECT 'x' AS a;
INSERT INTO FUNCTION s3('$path/t2/dt=2026-01-01/ok.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(5) AS id;
SELECT 'inference without skip:';
DESCRIBE s3('$path/t2/**.parquet', 'test', 'testtest', 'Parquet') SETTINGS describe_compact_output = 1;
SELECT 'inference with skip:';
DESCRIBE s3('$path/t2/**.parquet', 'test', 'testtest', 'Parquet') SETTINGS s3_skip_hidden_files = 1, describe_compact_output = 1;
"
