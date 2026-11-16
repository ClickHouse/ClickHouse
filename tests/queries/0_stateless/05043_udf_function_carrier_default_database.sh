#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL UDF expansion happens after the table names of a CREATE query are qualified with the current
# database, so the names which the expansion brings in have to be qualified separately before the
# query is persisted. Besides table expressions, the names can be carried by function arguments:
# the right argument of `IN` and the first argument of `dictGet`.

# A `SQL UDF` is a server-wide object, so its name has to be unique across concurrently running tests.
UDF_IN="${CLICKHOUSE_DATABASE}_udf_in"
UDF_DICT="${CLICKHOUSE_DATABASE}_udf_dict"
DB2="${CLICKHOUSE_DATABASE}_1"

$CLICKHOUSE_CLIENT --query "
CREATE DATABASE ${DB2};

CREATE TABLE ${DB2}.in_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${DB2}.in_source VALUES (1);

CREATE TABLE ${DB2}.dict_source (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${DB2}.dict_source VALUES (1, 42);
CREATE DICTIONARY ${DB2}.dict (id UInt64, value UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(DATABASE '${DB2}' TABLE 'dict_source'))
LAYOUT(FLAT())
LIFETIME(0);

USE ${DB2};
CREATE FUNCTION ${UDF_IN} AS (x) -> x IN in_source;
CREATE FUNCTION ${UDF_DICT} AS (x) -> dictGet('dict', 'value', x);

CREATE VIEW view_in AS SELECT ${UDF_IN}(1) AS r;
CREATE VIEW view_dict AS SELECT ${UDF_DICT}(toUInt64(1)) AS r;

-- The persisted view queries have to carry the qualified names.
SELECT create_table_query LIKE '%IN (%' || currentDatabase() || '.in_source%' FROM system.tables WHERE database = currentDatabase() AND name = 'view_in';
SELECT create_table_query LIKE '%dictGet(''' || currentDatabase() || '.dict''%' FROM system.tables WHERE database = currentDatabase() AND name = 'view_dict';
"

# Reading the views from a different current database has to keep resolving the names
# against the database of the CREATE query.
$CLICKHOUSE_CLIENT --query "
USE ${CLICKHOUSE_DATABASE};
SELECT * FROM ${DB2}.view_in;
SELECT * FROM ${DB2}.view_dict;
"

$CLICKHOUSE_CLIENT --query "
DROP VIEW ${DB2}.view_in;
DROP VIEW ${DB2}.view_dict;
DROP FUNCTION ${UDF_IN};
DROP FUNCTION ${UDF_DICT};
DROP DICTIONARY ${DB2}.dict;
DROP TABLE ${DB2}.dict_source;
DROP TABLE ${DB2}.in_source;
DROP DATABASE ${DB2};
"
