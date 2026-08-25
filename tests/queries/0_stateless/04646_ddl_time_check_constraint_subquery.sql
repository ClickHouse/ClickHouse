-- Tags: no-async-insert, no-parallel
-- - no-async-insert -- with wait_for_async_insert=0 the INSERT is fire-and-forget, so the constraint error is raised in the background flush and never reaches the client, breaking the { serverError } assertion.
-- - no-parallel -- SQL UDFs are global server objects; the flaky check runs the same test concurrently and the CREATE FUNCTION statements would collide.

-- A `CHECK` constraint containing a subquery can never be evaluated, so it is rejected on the DDL path
-- and never persisted into the table metadata. Only a direct subquery on the set side of an `IN`-family
-- operator is allowed: it becomes a set built lazily at insert time.

-- CREATE TABLE
CREATE TABLE ddl_check_bare (c0 Int, CONSTRAINT c0 CHECK (SELECT 1)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ddl_check_scalar (c0 Int, CONSTRAINT c0 CHECK equals((SELECT 1), c0)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- Validation must happen before `TreeRewriter` can execute the scalar subquery.
CREATE TABLE ddl_check_scalar_throw (c0 Int, CONSTRAINT c0 CHECK equals((SELECT throwIf(1) FROM numbers(1)), c0)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ddl_check_in_lhs (c0 Int, CONSTRAINT c0 CHECK (SELECT 1) IN (1, 2, 3)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ddl_check_in_set_list (c0 Int, CONSTRAINT c0 CHECK c0 IN (1, (SELECT 1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'ddl_check_%';

-- ALTER TABLE ADD/MODIFY CONSTRAINT
CREATE TABLE ddl_alter (c0 Int, CONSTRAINT c0 CHECK c0 > 0) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE ddl_alter ADD CONSTRAINT c1 CHECK equals((SELECT 1), c0); -- { serverError BAD_ARGUMENTS }
ALTER TABLE ddl_alter MODIFY CONSTRAINT c0 CHECK equals((SELECT 1), c0); -- { serverError BAD_ARGUMENTS }
-- The rejected commands left the metadata alone, so the table still works.
INSERT INTO ddl_alter VALUES (1);
INSERT INTO ddl_alter VALUES (-1); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ddl_alter;

-- A subquery hidden inside a SQL user-defined function body is rejected as well: the Analyzer inlines the
-- function body, so the subquery would be executed on every insert.
DROP FUNCTION IF EXISTS udf_with_subquery_04646;
CREATE FUNCTION udf_with_subquery_04646 AS (x) -> equals((SELECT 1), x);
CREATE TABLE ddl_udf (c0 Int, CONSTRAINT c0 CHECK udf_with_subquery_04646(c0)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
DROP FUNCTION udf_with_subquery_04646;

-- A direct subquery on the set side of `IN` remains allowed.
CREATE TABLE ddl_in_set_src (id Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ddl_in_set_src VALUES (1);
CREATE TABLE ddl_in_set (c0 Int, CONSTRAINT c0 CHECK c0 IN (SELECT id FROM ddl_in_set_src)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ddl_in_set VALUES (1);
INSERT INTO ddl_in_set VALUES (2); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ddl_in_set;

-- Metadata written before this validation existed keeps loading: the check does not run for the short
-- `ATTACH TABLE t` form (or on server startup), only the compilation of the constraint expression rejects
-- it, on the first insert into such a table. A full-definition `ATTACH TABLE t (...) ENGINE = ...` is fresh
-- user input and is rejected -- see `04671_attach_full_definition_check_constraint_subquery`.

-- A bare identifier on the set side of `IN` is a table name, even when a column of the table has that name:
-- the DDL interpreters run `AddDefaultDatabaseVisitor` over the constraint before it is stored, and that
-- visitor qualifies the set-side identifier with the current database (`c0 IN c1` becomes `c0 IN default.c1`).
-- Such a constraint can only ever be a table-backed set, which has no pipeline to materialize it, so it is
-- rejected on the DDL path instead of failing on every insert.
CREATE TABLE ddl_in_table (c0 Int, CONSTRAINT c0 CHECK c0 IN ddl_in_set_src) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ddl_in_column (c0 Int, c1 Array(Int), CONSTRAINT c0 CHECK c0 IN c1) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
ALTER TABLE ddl_alter ADD CONSTRAINT c2 CHECK c0 IN ddl_in_set_src; -- { serverError BAD_ARGUMENTS }

-- The same rule applies when the `IN` operator is hidden inside a SQL user-defined function body:
-- UDF expansion happens after `AddDefaultDatabaseVisitor`, so the expanded constraint carries a bare
-- (unqualified) set-side identifier. Such a constraint never worked: released versions persist it and
-- then fail every insert with `UNKNOWN_IDENTIFIER`, so it is rejected on the DDL path as well.
DROP FUNCTION IF EXISTS udf_in_carrier_04646;
CREATE FUNCTION udf_in_carrier_04646 AS (x, arr) -> x IN arr;
CREATE TABLE ddl_udf_in (c0 Int, c1 Array(Int), CONSTRAINT c0 CHECK udf_in_carrier_04646(c0, c1)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
DROP FUNCTION udf_in_carrier_04646;

-- Membership in an array column is expressed with `has`, which is a plain function and is allowed.
CREATE TABLE ddl_has_column (c0 Int, c1 Array(Int), CONSTRAINT c0 CHECK has(c1, c0)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ddl_has_column VALUES (1, [1, 2]);
INSERT INTO ddl_has_column VALUES (3, [1, 2]); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ddl_has_column;
