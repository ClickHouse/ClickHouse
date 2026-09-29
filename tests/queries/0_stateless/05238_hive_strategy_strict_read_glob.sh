#!/usr/bin/env bash
# With hive_partition_strategy_strict_read_glob, a partition_strategy='hive' table reads only
# paths laid out exactly as it writes them (`key1=*/.../keyN=*/<file>`): staging output of
# Hive-ecosystem writers (`_temporary/...`, `.spark-staging-<id>/...`), files at other depths
# and files under non-`key=value` directories are excluded structurally. Without the setting
# the recursive glob reads all of them.
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05238"

$CLICKHOUSE_CLIENT -q "
-- One committed file plus files a strict layout must not read: Spark staging output, a file
-- under a non-partition directory and a file nested below the partition directories.
INSERT INTO FUNCTION s3('$path/t/org=a/dt=2026-01-01/x.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(1) AS id;
INSERT INTO FUNCTION s3('$path/t/_temporary/0/_temporary/attempt_x/org=a/dt=2026-01-01/y.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(2) AS id;
INSERT INTO FUNCTION s3('$path/t/.spark-staging-y/org=a/dt=2026-01-01/z.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(3) AS id;
INSERT INTO FUNCTION s3('$path/t/junk/org=a/dt=2026-01-01/w.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(4) AS id;
INSERT INTO FUNCTION s3('$path/t/org=a/dt=2026-01-01/extra/v.parquet', 'test', 'testtest', 'Parquet') SELECT toInt64(5) AS id;

CREATE TABLE 05238_recursive (id Int64, org String, dt String)
ENGINE = S3('$path/t', 'test', 'testtest', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY (org, dt);

SELECT 'recursive glob count:', count() FROM 05238_recursive;

SET hive_partition_strategy_strict_read_glob = 1;

CREATE TABLE 05238_strict (id Int64, org String, dt String)
ENGINE = S3('$path/t', 'test', 'testtest', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY (org, dt);

SELECT 'strict glob rows:', id, org, dt FROM 05238_strict ORDER BY id;
SELECT 'strict glob filtered count:', count() FROM 05238_strict WHERE dt = '2026-01-01';

-- Files written through the table match the strict shape and are read back.
INSERT INTO 05238_strict VALUES (6, 'b', '2026-01-02');
SELECT 'strict glob after insert:', id, org, dt FROM 05238_strict ORDER BY id;

DROP TABLE 05238_recursive;
DROP TABLE 05238_strict;
"

# Partition column names with glob metacharacters cannot form a strict glob.
$CLICKHOUSE_CLIENT --hive_partition_strategy_strict_read_glob=1 -q "
CREATE TABLE 05238_bad_name (id Int64, \`o*rg\` String)
ENGINE = S3('$path/t2', 'test', 'testtest', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY \`o*rg\`;
" 2>&1 | grep -o -m1 "glob metacharacters"
