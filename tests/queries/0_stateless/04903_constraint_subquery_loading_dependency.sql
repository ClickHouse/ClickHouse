-- A table read by a scalar subquery inside a CONSTRAINT is read while the dependent table is being
-- attached, so it must be a loading dependency: otherwise it can be dropped and the server does not
-- start anymore.

DROP TABLE IF EXISTS t_constraint_dep_source;
DROP TABLE IF EXISTS t_constraint_dep_user;
DROP TABLE IF EXISTS t_constraint_in_source;
DROP TABLE IF EXISTS t_constraint_in_user;

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
