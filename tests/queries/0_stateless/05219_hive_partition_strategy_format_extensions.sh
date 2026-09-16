#!/usr/bin/env bash
# With partition_strategy='hive', the read glob used to be built from the lowercased format
# name. For any format whose name is not the extension real files carry (JSONEachRow over
# .jsonl, CSVWithNames over .csv, ...), the glob matched nothing and the table was silently
# empty. The glob must match every file extension registered for the format, and the files
# written by ClickHouse itself (named <snowflake id>.<lowercased format name>) must keep
# matching.
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05219"

$CLICKHOUSE_CLIENT -q "
-- A pre-existing lake of .jsonl files. The uploaded files do not contain the partition column,
-- its value lives in the path.
INSERT INTO FUNCTION s3('$path/jsonl_lake/key=1/data.jsonl', 'test', 'testtest', 'JSONEachRow') SELECT 1 AS id;

CREATE TABLE 05219_jsonl (id UInt64, key UInt64)
ENGINE = S3('$path/jsonl_lake', 'test', 'testtest', format = 'JSONEachRow', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'jsonl lake:';
SELECT id, key FROM 05219_jsonl;

-- A pre-existing lake of .csv files with a header row.
INSERT INTO FUNCTION s3('$path/csv_lake/key=2/data.csv', 'test', 'testtest', 'CSVWithNames') SELECT 42 AS id;

CREATE TABLE 05219_csv (id UInt64, key UInt64)
ENGINE = S3('$path/csv_lake', 'test', 'testtest', format = 'CSVWithNames', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'csv lake:';
SELECT id, key FROM 05219_csv;

-- Files written through the table itself are named <snowflake id>.<lowercased format name>
-- and must still be read back together with the pre-existing files.
INSERT INTO 05219_jsonl VALUES (3, 4);

SELECT 'jsonl lake after insert:';
SELECT id, key FROM 05219_jsonl ORDER BY id;

DROP TABLE 05219_jsonl;
DROP TABLE 05219_csv;
"
