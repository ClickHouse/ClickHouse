#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The first argument of `joinGet` carries a table name. When it is brought into a CREATE query by
# SQL UDF expansion it is not qualified with the current database yet, and `joinGet` resolves a bare
# name against the current database of the query which reads the view or inserts into the table -
# so a reader with another current database would look the table up in the wrong database. The name
# has to be qualified before the query is persisted, in the identifier form and in the string form.

# A `SQL UDF` is a server-wide object, so its name has to be unique across concurrently running tests.
UDF_IDENTIFIER="${CLICKHOUSE_DATABASE}_udf_join_identifier"
UDF_STRING="${CLICKHOUSE_DATABASE}_udf_join_string"
DB2="${CLICKHOUSE_DATABASE}_1"

$CLICKHOUSE_CLIENT --query "
CREATE DATABASE ${DB2};

CREATE TABLE ${DB2}.join_source (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO ${DB2}.join_source VALUES (1, 'right');

-- A table with the same name in the database of the reader: it must not be picked up.
CREATE TABLE join_source (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_source VALUES (1, 'wrong');

USE ${DB2};
CREATE FUNCTION ${UDF_IDENTIFIER} AS (k) -> joinGet(join_source, 'v', k);
CREATE FUNCTION ${UDF_STRING} AS (k) -> joinGet('join_source', 'v', k);

CREATE VIEW view_join AS SELECT ${UDF_IDENTIFIER}(toUInt64(1)) AS identifier_form, ${UDF_STRING}(toUInt64(1)) AS string_form;
CREATE TABLE table_join (k UInt64, v String DEFAULT ${UDF_IDENTIFIER}(k)) ENGINE = Memory;

-- The persisted definitions have to carry the qualified names.
SELECT create_table_query LIKE '%joinGet(' || currentDatabase() || '.join_source%' FROM system.tables WHERE database = currentDatabase() AND name = 'view_join';
SELECT create_table_query LIKE '%joinGet(''' || currentDatabase() || '.join_source''%' FROM system.tables WHERE database = currentDatabase() AND name = 'view_join';
SELECT create_table_query LIKE '%joinGet(' || currentDatabase() || '.join_source%' FROM system.tables WHERE database = currentDatabase() AND name = 'table_join';

-- And the table depends on the Join table it reads for loading.
SELECT loading_dependencies_database = [currentDatabase()], loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'table_join';
"

# Reading the view and inserting into the table from a different current database, which has a Join
# table of the same name, has to keep resolving the name against the database of the CREATE query.
$CLICKHOUSE_CLIENT --query "
USE ${CLICKHOUSE_DATABASE};
SELECT * FROM ${DB2}.view_join;
INSERT INTO ${DB2}.table_join (k) VALUES (1);
SELECT * FROM ${DB2}.table_join;
"

$CLICKHOUSE_CLIENT --query "
DROP TABLE ${DB2}.table_join;
DROP VIEW ${DB2}.view_join;
DROP FUNCTION ${UDF_IDENTIFIER};
DROP FUNCTION ${UDF_STRING};
DROP TABLE ${DB2}.join_source;
DROP TABLE join_source;
DROP DATABASE ${DB2};
"
