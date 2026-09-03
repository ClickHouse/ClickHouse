#!/usr/bin/env bash
# Tags: no-fasttest

# Tests for parsing JSON from Parquet files into CH JSON columns
# and writing CH JSON columns back to Parquet.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

###############################################################################
#  A1.  ClickHouse -> Parquet -> ClickHouse (round-trip) (Default JSON)
###############################################################################

PAR_FILE_A="${CLICKHOUSE_TMP}/example_json_roundtrip.parquet"
EXAMPLE_ORIG="${CLICKHOUSE_TMP}/example_orig.dump"
EXAMPLE_BACK="${CLICKHOUSE_TMP}/example_back.dump"
 
# 1) Original dump directly from ClickHouse (without Parquet)
$CLICKHOUSE_LOCAL -n > "${EXAMPLE_ORIG}" <<'SQL'
CREATE TABLE example
(
    id       Int32,
    json_str String,
    json_val JSON
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}',         '{"user":"alice","age":30}'),
    (2, '{"items":[1,2,3],"flag":true}',     '{"items":{"yes":[1,2,3]},"flag":true}'),
    (3, '{"meta":{"x":10,"y":20}}',          NULL);

SELECT * FROM example ORDER BY id FORMAT TSV;
SQL

# 2) Write Parquet File that contains JSON
$CLICKHOUSE_LOCAL -n > "${PAR_FILE_A}" <<'SQL'
CREATE TABLE example
(
    id       Int32,
    json_str String,
    json_val JSON
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}',         '{"user":"alice","age":30}'),
    (2, '{"items":[1,2,3],"flag":true}',     '{"items":{"yes":[1,2,3]},"flag":true}'),
    (3, '{"meta":{"x":10,"y":20}}',          NULL);

SELECT * FROM example ORDER BY id FORMAT Parquet;
SQL

# 3) read back with JSON parsing enabled and dump to file
$CLICKHOUSE_LOCAL -q "
    SET input_format_parquet_enable_json_parsing = 1;
    SELECT * FROM file('${PAR_FILE_A}', Parquet)
    ORDER BY id FORMAT TSV" \
    > "${EXAMPLE_BACK}"

# 4) Compare original and back-parsed data
echo "diff_roundtrip:"
diff "${EXAMPLE_ORIG}" "${EXAMPLE_BACK}"

# Sanity check: parsing enabled -> column is JSON
$CLICKHOUSE_LOCAL -q "
    SET input_format_parquet_enable_json_parsing = 1;
    SELECT toTypeName(json_val) FROM file('${PAR_FILE_A}', Parquet) LIMIT 1;"

# Sanity check: parsing disabled -> column is String
$CLICKHOUSE_LOCAL -q "
    SET input_format_parquet_enable_json_parsing = 0;
    SELECT toTypeName(json_val) FROM file('${PAR_FILE_A}', Parquet) LIMIT 1;"


###############################################################################
#  A2.  ClickHouse -> Parquet -> ClickHouse (round-trip) (Non-Default JSON)
###############################################################################

PAR_FILE_A="${CLICKHOUSE_TMP}/example_json_roundtrip2.parquet"
EXAMPLE_ORIG="${CLICKHOUSE_TMP}/example_orig2.dump"
EXAMPLE_BACK="${CLICKHOUSE_TMP}/example_back2.dump"
 
# 1) Original dump directly from ClickHouse (without Parquet)
$CLICKHOUSE_LOCAL -n > "${EXAMPLE_ORIG}" <<'SQL'
CREATE TABLE example
(
    id       Int32,
    json_str String,
    json_val JSON(max_dynamic_types = 1, max_dynamic_paths = 1, age UInt32, flag Bool)
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}',         '{"user":"alice","age":"30"}'),
    (2, '{"items":[1,2,3],"flag":true}',     '{"items":{"yes":[1,2,3]},"flag":true}'),
    (3, '{"meta":{"x":10,"y":20}}',          NULL);

SELECT * FROM example ORDER BY id FORMAT TSV;
SQL

# 2) Write Parquet File that contains JSON
$CLICKHOUSE_LOCAL -n > "${PAR_FILE_A}" <<'SQL'
CREATE TABLE example
(
    id       Int32,
    json_str String,
    json_val JSON(max_dynamic_types = 1, max_dynamic_paths = 1, age UInt32, flag Bool)
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}',         '{"user":"alice","age":"30"}'),
    (2, '{"items":[1,2,3],"flag":true}',     '{"items":{"yes":[1,2,3]},"flag":true}'),
    (3, '{"meta":{"x":10,"y":20}}',          NULL);

SELECT * FROM example ORDER BY id FORMAT Parquet;
SQL

# 3) read back with JSON parsing enabled and dump to file
$CLICKHOUSE_LOCAL -q "
    SET input_format_parquet_enable_json_parsing = 1;
    SELECT *
    FROM file(
        '${PAR_FILE_A}',
        Parquet,
        'id Int32,
         json_str String,
         json_val JSON(max_dynamic_types = 1,
                       max_dynamic_paths = 1,
                       age UInt32,
                       flag Bool)'
    )
    ORDER BY id
    FORMAT TSV
" > "${EXAMPLE_BACK}"

# 4) Compare original and back-parsed data
echo "diff_roundtrip:"
diff "${EXAMPLE_ORIG}" "${EXAMPLE_BACK}"

# Sanity check: parsing enabled -> column is JSON
$CLICKHOUSE_LOCAL -q "
    SET input_format_parquet_enable_json_parsing = 1;
    SELECT toTypeName(json_val) FROM file('${PAR_FILE_A}', Parquet) LIMIT 1;"

# Sanity check: parsing disabled -> column is String
$CLICKHOUSE_LOCAL -q "
    SET input_format_parquet_enable_json_parsing = 0;
    SELECT toTypeName(json_val) FROM file('${PAR_FILE_A}', Parquet) LIMIT 1;"


###############################################################################
#  B.  Read an external Parquet file that already contains JSON
###############################################################################
PAR_FILE_B_REL="data_parquet/sample_json.parquet"
PAR_FILE_B="${CUR_DIR}/${PAR_FILE_B_REL}"

# Verify that the json_val column is stored as a JSON Column
$CLICKHOUSE_LOCAL -q "
SELECT
    tupleElement(c, 'physical_type') AS physical_type,
    tupleElement(c, 'logical_type')  AS logical_type
FROM file('${PAR_FILE_B}', ParquetMetaData)
ARRAY JOIN columns AS c
WHERE tupleElement(c, 'name') = 'json_val';"

# Read WITHOUT parsing – json_val is treated as String
$CLICKHOUSE_LOCAL -q "
SET input_format_parquet_enable_json_parsing = 0;
SELECT  toTypeName(json_val) AS col_type,
        json_val             AS sample_val
FROM    file('${PAR_FILE_B}', Parquet)
ORDER BY id
LIMIT 2;"

# Read WITH parsing – json_val is now JSON
$CLICKHOUSE_LOCAL -q "
SET input_format_parquet_enable_json_parsing = 1;
SELECT  toTypeName(json_val)    AS col_type,
        json_val.user.name      AS user_name
FROM    file('${PAR_FILE_B}', Parquet)
ORDER BY id
LIMIT 2;"

# Explicit schema beats the flag: force json_val to String
$CLICKHOUSE_LOCAL -q "
SELECT  toTypeName(json_val) AS forced_type
FROM file(
        '${PAR_FILE_B}',
        Parquet,
        'id Int32, json_str String, json_val String')
LIMIT 1;"

# Verify that the json_str column is stored as a String Column
$CLICKHOUSE_LOCAL -q "
SELECT
    tupleElement(c, 'physical_type') AS physical_type,
    tupleElement(c, 'logical_type')  AS logical_type
FROM file('${PAR_FILE_B}', ParquetMetaData)
ARRAY JOIN columns AS c
WHERE tupleElement(c, 'name') = 'json_str';"

# Force json_str to be JSON (regardless of the flag)
$CLICKHOUSE_LOCAL -q "
SET input_format_parquet_enable_json_parsing = 0;
SELECT  toTypeName(json_str)    AS col_type,
        json_str.user.name      AS user_name
FROM    file(
            '${PAR_FILE_B}',
             Parquet,
             'id Int32, json_str JSON, json_val String')
ORDER BY id
LIMIT 2;"
