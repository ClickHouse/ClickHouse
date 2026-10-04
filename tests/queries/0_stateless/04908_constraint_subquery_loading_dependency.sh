#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `SQL UDF` is a server-wide object, so its name has to be unique across concurrently running tests.
UDF="${CLICKHOUSE_DATABASE}_udf"
DB2="${CLICKHOUSE_DATABASE}_1"

# A table read by a scalar subquery inside a CONSTRAINT is read while the dependent table is being
# attached, so it must be a loading dependency: otherwise it can be dropped and the server does not
# start anymore. Such a constraint is rejected by CREATE TABLE, but metadata written before that
# validation existed keeps loading. To obtain it, create the table with a valid constraint in a
# `clickhouse local` session with a persistent path, rewrite the constraint in the stored metadata
# file, and start a second session.

WORK_DIR=$CLICKHOUSE_TMP/04908_constraint_subquery_loading_dependency
rm -rf "$WORK_DIR"

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
CREATE TABLE t_constraint_dep_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_constraint_dep_user (x UInt64, CONSTRAINT c CHECK x < 1000) ENGINE = MergeTree ORDER BY tuple();
"

sed -i "s/CHECK x < 1000/CHECK x < (SELECT max(id) + 1000 FROM default.t_constraint_dep_source)/" "$WORK_DIR"/store/*/*/t_constraint_dep_user.sql

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 't_constraint_dep_user';
DROP TABLE t_constraint_dep_source; -- { serverError HAVE_DEPENDENT_OBJECTS }
"

rm -rf "$WORK_DIR"

$CLICKHOUSE_CLIENT --query "
-- A subquery in the right argument of IN is not executed while the table is attached, so it stays
-- out of the loading dependencies and the table it reads can be dropped.

CREATE TABLE t_constraint_in_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_constraint_in_user (x UInt64, CONSTRAINT c CHECK x IN (SELECT id FROM t_constraint_in_source)) ENGINE = MergeTree ORDER BY tuple();

SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 't_constraint_in_user';

DROP TABLE t_constraint_in_source;

DROP TABLE t_constraint_in_user;
"

# SQL UDF expansion happens after database qualification. The subquery introduced by the UDF still
# has to use the CREATE query's current database.

$CLICKHOUSE_CLIENT --query "
CREATE DATABASE ${DB2};
CREATE TABLE ${DB2}.source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${DB2}.source VALUES (1);
USE ${DB2};
CREATE FUNCTION ${UDF} AS () -> (SELECT max(id) + 1000 FROM source);
-- The scalar subquery gets an explicit alias: a view whose only projection is an unaliased scalar
-- subquery cannot be read at all, and that is an unrelated pre-existing issue.
CREATE VIEW udf_view AS SELECT ${UDF}() AS v;

SELECT create_table_query LIKE '%' || currentDatabase() || '.source%' FROM system.tables WHERE database = currentDatabase() AND name = 'udf_view';

USE ${CLICKHOUSE_DATABASE};
SELECT * FROM ${DB2}.udf_view;

USE ${DB2};
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
