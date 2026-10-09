#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `SQL UDF` is a server-wide object, so its name has to be unique across concurrently running tests.
UDF="${CLICKHOUSE_DATABASE}_udf"
DB2="${CLICKHOUSE_DATABASE}_1"

$CLICKHOUSE_CLIENT --query "
-- A table read by a scalar subquery inside a CONSTRAINT is read while the dependent table is being
-- attached, so it must be a loading dependency: otherwise it can be dropped and the server does not
-- start anymore.

CREATE TABLE t_constraint_dep_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_constraint_dep_user (x UInt64, CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM t_constraint_dep_source)) ENGINE = MergeTree ORDER BY tuple();

SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 't_constraint_dep_user';

DROP TABLE t_constraint_dep_source; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- A subquery in the right argument of IN is not executed while the table is attached, so it stays
-- out of the loading dependencies and the table it reads can be dropped.

CREATE TABLE t_constraint_in_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_constraint_in_user (x UInt64, CONSTRAINT c CHECK x IN (SELECT id FROM t_constraint_in_source)) ENGINE = MergeTree ORDER BY tuple();

SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 't_constraint_in_user';

DROP TABLE t_constraint_in_source;

DROP TABLE t_constraint_dep_user;
DROP TABLE t_constraint_dep_source;
DROP TABLE t_constraint_in_user;
"

# SQL UDF expansion happens after database qualification. The subquery introduced by the UDF still
# has to use the CREATE query's current database when collecting loading dependencies.

$CLICKHOUSE_CLIENT --query "
CREATE DATABASE ${DB2};
CREATE TABLE ${DB2}.source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${DB2}.source VALUES (1);
USE ${DB2};
CREATE FUNCTION ${UDF} AS () -> (SELECT max(id) + 1000 FROM source);
CREATE TABLE user (x UInt64, CONSTRAINT c CHECK x < ${UDF}()) ENGINE = MergeTree ORDER BY tuple();
-- The scalar subquery gets an explicit alias: a view whose only projection is an unaliased scalar
-- subquery cannot be read at all, and that is an unrelated pre-existing issue.
CREATE VIEW udf_view AS SELECT ${UDF}() AS v;

SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'user';
SELECT create_table_query LIKE '%' || currentDatabase() || '.source%' FROM system.tables WHERE database = currentDatabase() AND name = 'udf_view';

DROP TABLE source; -- { serverError HAVE_DEPENDENT_OBJECTS }

DETACH TABLE user;
USE ${CLICKHOUSE_DATABASE};
ATTACH TABLE ${DB2}.user;
SELECT * FROM ${DB2}.udf_view;

USE ${DB2};
DROP TABLE user;
DROP VIEW udf_view;
DROP FUNCTION ${UDF};
"

# The nested SELECT of a dictionary source is executed with the global context, so its unqualified
# table names keep resolving against the default database of the server, not against the database
# of the dictionary.

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${DB2}.dict (id UInt64, value UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT id, id AS value FROM dictionary_source_for_04908'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT loading_dependencies_database, loading_dependencies_table FROM system.tables WHERE database = '${DB2}' AND name = 'dict';

DROP DICTIONARY ${DB2}.dict;
DROP DATABASE ${DB2};
"
