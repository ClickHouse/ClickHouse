#!/usr/bin/env bash
# With partition_strategy='hive', the read glob used to be built from the lowercased format
# name. For any format whose name is not the extension real files carry (JSONEachRow over
# .json / .jsonl, CSVWithNames over .csv, TabSeparated over .tsv, ...), the glob matched
# nothing and the table was silently empty. The glob must match every file extension
# registered for the format, and the files written by ClickHouse itself (named
# <snowflake id>.<lowercased format name>) must keep matching.
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05233"

$CLICKHOUSE_CLIENT -q "
-- A pre-existing lake of .jsonl files. The uploaded files do not contain the partition column,
-- its value lives in the path.
INSERT INTO FUNCTION s3('$path/jsonl_lake/key=1/data.jsonl', 'test', 'testtest', 'JSONEachRow') SELECT 1 AS id;

CREATE TABLE 05233_jsonl (id UInt64, key UInt64)
ENGINE = S3('$path/jsonl_lake', 'test', 'testtest', format = 'JSONEachRow', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'jsonl lake:';
SELECT id, key FROM 05233_jsonl;

-- NDJSON lakes commonly name their files .json, although the extension itself infers as the
-- JSON format.
INSERT INTO FUNCTION s3('$path/json_lake/key=3/data.json', 'test', 'testtest', 'JSONEachRow') SELECT 7 AS id;

CREATE TABLE 05233_json (id UInt64, key UInt64)
ENGINE = S3('$path/json_lake', 'test', 'testtest', format = 'JSONEachRow', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'json lake:';
SELECT id, key FROM 05233_json;

-- A pre-existing lake of .csv files with a header row.
INSERT INTO FUNCTION s3('$path/csv_lake/key=2/data.csv', 'test', 'testtest', 'CSVWithNames') SELECT 42 AS id;

CREATE TABLE 05233_csv (id UInt64, key UInt64)
ENGINE = S3('$path/csv_lake', 'test', 'testtest', format = 'CSVWithNames', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'csv lake:';
SELECT id, key FROM 05233_csv;

-- TabSeparated and TSV are registered as independent spellings of the same format, and the
-- read glob must not depend on which one the user typed.
INSERT INTO FUNCTION s3('$path/tsv_lake/key=5/data.tsv', 'test', 'testtest', 'TSV') SELECT 8 AS id;

CREATE TABLE 05233_tsv (id UInt64, key UInt64)
ENGINE = S3('$path/tsv_lake', 'test', 'testtest', format = 'TabSeparated', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'tsv lake:';
SELECT id, key FROM 05233_tsv;

-- Interchangeable spellings of the same format must build the same glob: JSONLines is
-- JSONEachRow and TSVRaw is TabSeparatedRaw, and the extensions are registered for the
-- canonical spelling only.
CREATE TABLE 05233_jsonlines (id UInt64, key UInt64)
ENGINE = S3('$path/jsonl_lake', 'test', 'testtest', format = 'JSONLines', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'jsonl lake as JSONLines:';
SELECT id, key FROM 05233_jsonlines;

CREATE TABLE 05233_tsvraw (id UInt64, key UInt64)
ENGINE = S3('$path/tsv_lake', 'test', 'testtest', format = 'TSVRaw', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'tsv lake as TSVRaw:';
SELECT id, key FROM 05233_tsvraw;

-- Files written through the table itself are named <snowflake id>.<lowercased format name>
-- and must still be read back together with the pre-existing files.
INSERT INTO 05233_jsonl VALUES (3, 4);

SELECT 'jsonl lake after insert:';
SELECT id, key FROM 05233_jsonl ORDER BY id;

DROP TABLE 05233_jsonl;
DROP TABLE 05233_json;
DROP TABLE 05233_csv;
DROP TABLE 05233_tsv;
DROP TABLE 05233_jsonlines;
DROP TABLE 05233_tsvraw;
"
