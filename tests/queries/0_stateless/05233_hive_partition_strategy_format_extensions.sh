#!/usr/bin/env bash
# With partition_strategy='hive', the read glob used to be built from the lowercased format
# name. For any format whose name is not the extension real files carry (JSONEachRow over
# .json / .jsonl, CSVWithNames over .csv, TabSeparated over .tsv, ...), the glob matched
# nothing and the table was silently empty. The glob must match every file extension
# registered for the format, together with the compression suffixes the reader would accept,
# and the files written by ClickHouse itself (named <snowflake id>.<lowercased format name>)
# must keep matching.
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

-- Every spelling of a format is a format of its own and writes files named after itself:
-- a lake written as JSONLines is a lake of .jsonlines files, and it must be readable as
-- JSONEachRow, so the alias lookup has to walk in both directions.
INSERT INTO FUNCTION s3('$path/jsonlines_lake/key=6/data.jsonlines', 'test', 'testtest', 'JSONLines') SELECT 9 AS id;

CREATE TABLE 05233_jsonlines_lake (id UInt64, key UInt64)
ENGINE = S3('$path/jsonlines_lake', 'test', 'testtest', format = 'JSONEachRow', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'jsonlines lake as JSONEachRow:';
SELECT id, key FROM 05233_jsonlines_lake;

-- The same holds for the WithNames flavours of an aliased format.
INSERT INTO FUNCTION s3('$path/tsvwithnames_lake/key=7/data.tsvwithnames', 'test', 'testtest', 'TSVWithNames') SELECT 10 AS id;

CREATE TABLE 05233_tsvwithnames_lake (id UInt64, key UInt64)
ENGINE = S3('$path/tsvwithnames_lake', 'test', 'testtest', format = 'TabSeparatedWithNames', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'tsvwithnames lake as TabSeparatedWithNames:';
SELECT id, key FROM 05233_tsvwithnames_lake;

-- Compressed lakes: the listing is filtered by the object key, long before anything
-- decompresses it, so the glob has to spell out the compression suffixes as well.
INSERT INTO FUNCTION s3('$path/gz_lake/key=8/data.jsonl.gz', 'test', 'testtest', 'JSONEachRow') SELECT 11 AS id;

CREATE TABLE 05233_gz (id UInt64, key UInt64)
ENGINE = S3('$path/gz_lake', 'test', 'testtest', format = 'JSONEachRow', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'gzipped jsonl lake:';
SELECT id, key FROM 05233_gz;

-- An explicit compression method is authoritative: the reader ignores the file name, so the glob
-- must accept any suffix after the format extension (a gzipped file does not have to be named
-- .gz), and it still has to match the files named without any suffix - that is what the table
-- itself writes.
INSERT INTO FUNCTION s3('$path/gz_lake/key=9/data.jsonl.custom', 'test', 'testtest', format = 'JSONEachRow', compression_method = 'gzip') SELECT 12 AS id;

CREATE TABLE 05233_gz_explicit (id UInt64, key UInt64)
ENGINE = S3('$path/gz_lake', 'test', 'testtest', format = 'JSONEachRow', compression_method = 'gzip', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'gzipped jsonl lake with an explicit compression method:';
SELECT id, key FROM 05233_gz_explicit ORDER BY id;

-- Autodetection goes by the file name, so it cannot know that .custom is gzip and sees the .gz
-- file only.
SELECT 'gzipped jsonl lake with autodetection:';
SELECT id, key FROM 05233_gz ORDER BY id;

-- The suffix is arbitrary, but it has to be a suffix: a sibling lake of .csvwithnames.gz files under
-- the same prefix must stay invisible to a CSV table, because the reader trusts the explicit codec
-- and never looks at the name, so a prefix match on the extension would hand the header row of
-- those files to the CSV parser as data.
INSERT INTO FUNCTION s3('$path/csv_gz_lake/key=10/data.csv.custom', 'test', 'testtest', format = 'CSV', compression_method = 'gzip') SELECT 13 AS id;
INSERT INTO FUNCTION s3('$path/csv_gz_lake/key=11/data.csvwithnames.gz', 'test', 'testtest', 'CSVWithNames') SELECT 14 AS id;

CREATE TABLE 05233_csv_gz_explicit (id UInt64, key UInt64)
ENGINE = S3('$path/csv_gz_lake', 'test', 'testtest', format = 'CSV', compression_method = 'gzip', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'gzipped csv lake with an explicit compression method and a CSVWithNames sibling:';
SELECT id, key FROM 05233_csv_gz_explicit ORDER BY id;

-- With compression_method = 'none' the files carry no compression layer: a suffix after the
-- format extension is foreign data, not an alternative spelling, and only the bare extensions
-- match.
CREATE TABLE 05233_gz_none (id UInt64, key UInt64)
ENGINE = S3('$path/gz_lake', 'test', 'testtest', format = 'JSONEachRow', compression_method = 'none', partition_strategy = 'hive')
PARTITION BY key;

SELECT 'gzipped jsonl lake with compression_method = none:';
SELECT count() FROM 05233_gz_none;

-- Files written through the table itself are named <snowflake id>.<lowercased format name>
-- and must still be read back together with the pre-existing files.
INSERT INTO 05233_jsonl VALUES (3, 4);

SELECT 'jsonl lake after insert:';
SELECT id, key FROM 05233_jsonl ORDER BY id;

-- The same holds under an explicit codec: the table still writes <snowflake id>.<lowercased format
-- name> without any compression suffix (the codec is fixed by the table, not by the name), so the
-- explicit-codec glob has to keep the bare extensions, or the table would not see its own writes.
INSERT INTO 05233_gz_explicit VALUES (15, 12);

SELECT 'gzipped jsonl lake with an explicit compression method after insert:';
SELECT id, key FROM 05233_gz_explicit ORDER BY id;

DROP TABLE 05233_jsonl;
DROP TABLE 05233_json;
DROP TABLE 05233_csv;
DROP TABLE 05233_tsv;
DROP TABLE 05233_jsonlines;
DROP TABLE 05233_tsvraw;
DROP TABLE 05233_jsonlines_lake;
DROP TABLE 05233_tsvwithnames_lake;
DROP TABLE 05233_gz;
DROP TABLE 05233_gz_explicit;
DROP TABLE 05233_csv_gz_explicit;
DROP TABLE 05233_gz_none;
"

# A misspelled compression method must be reported, not silently turned into a glob without any
# compression suffix: table reads do not throw on zero matching files, so the table would just be
# empty.
echo 'invalid compression method:'
$CLICKHOUSE_CLIENT -q "
CREATE TABLE 05233_bad_codec (id UInt64, key UInt64)
ENGINE = S3('$path/gz_lake', 'test', 'testtest', format = 'JSONEachRow', compression_method = 'not_a_codec', partition_strategy = 'hive')
PARTITION BY key;
" 2>&1 | grep -cm1 "Unknown compression method 'not_a_codec'"
