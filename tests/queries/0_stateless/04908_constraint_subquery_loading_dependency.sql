-- A table read by a scalar subquery inside a CONSTRAINT is read while the dependent table is being
-- attached, so it must be a loading dependency: otherwise it can be dropped and the server does not
-- start anymore.

DROP TABLE IF EXISTS t_constraint_dep_source;
DROP TABLE IF EXISTS t_constraint_dep_user;
DROP TABLE IF EXISTS t_constraint_in_source;
DROP TABLE IF EXISTS t_constraint_in_user;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

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

-- SQL UDF expansion happens after database qualification. The subquery introduced by the UDF still
-- has to use the CREATE query's current database when collecting loading dependencies.

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.source VALUES (1);
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE FUNCTION constraint_udf_dependency_f AS () -> (SELECT max(id) + 1000 FROM source);
CREATE TABLE user (x UInt64, CONSTRAINT c CHECK x < constraint_udf_dependency_f()) ENGINE = MergeTree ORDER BY tuple();
-- The scalar subquery gets an explicit alias: a view whose only projection is an unaliased scalar
-- subquery cannot be read at all, and that is an unrelated pre-existing issue.
CREATE VIEW udf_view AS SELECT constraint_udf_dependency_f() AS v;

SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'user';
SELECT create_table_query LIKE '%' || currentDatabase() || '.source%' FROM system.tables WHERE database = currentDatabase() AND name = 'udf_view';

DROP TABLE source; -- { serverError HAVE_DEPENDENT_OBJECTS }

DETACH TABLE user;
USE default;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.user;
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.udf_view;

USE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE user;
DROP VIEW udf_view;
DROP FUNCTION constraint_udf_dependency_f;
USE default;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
