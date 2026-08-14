-- Tags: no-ordinary-database, no-replicated-database
-- Tag no-replicated-database: Unsupported type of CREATE TABLE ... CLONE AS ... query

-- A bare identifier as the right-hand side of `IN` in a constraint is rejected. Before, it was
-- rewritten to a table reference and persisted that way, leaving a table no INSERT could ever reach.

DROP TABLE IF EXISTS c1;
DROP TABLE IF EXISTS c2;
DROP TABLE IF EXISTS c3;
DROP TABLE IF EXISTS c4;
DROP TABLE IF EXISTS c5;
DROP TABLE IF EXISTS c6;
DROP TABLE IF EXISTS c7;
DROP TABLE IF EXISTS c8;
DROP TABLE IF EXISTS c9;
DROP TABLE IF EXISTS c10;
DROP TABLE IF EXISTS c11;
DROP TABLE IF EXISTS ok1;
DROP TABLE IF EXISTS ok2;
DROP TABLE IF EXISTS ok3;
DROP TABLE IF EXISTS ok4;
DROP TABLE IF EXISTS ok5;
DROP TABLE IF EXISTS ok6;
DROP TABLE IF EXISTS ok7;
DROP TABLE IF EXISTS ok8;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS s1;

CREATE TABLE c1 (a UInt16, b UInt16, CONSTRAINT c CHECK a IN (b))
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }
CREATE TABLE c2 (a UInt16, CONSTRAINT c CHECK 1 IN (a))
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }
CREATE TABLE c3 (a UInt16, b UInt16, CONSTRAINT c CHECK a NOT IN (b))
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }
CREATE TABLE c4 (a UInt16, b UInt16, CONSTRAINT c ASSUME a IN (b))
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }
CREATE TABLE c5 (conv UInt16, CONSTRAINT c CHECK (SELECT 1) IN (conv))
    ENGINE = MergeTree ORDER BY conv; -- { serverError BAD_ARGUMENTS }

CREATE TABLE c6 (a UInt16, b UInt16) ENGINE = MergeTree ORDER BY a;
ALTER TABLE c6 ADD CONSTRAINT c CHECK a IN (b); -- { serverError BAD_ARGUMENTS }
CREATE TABLE c7 (a UInt16, b UInt16, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a;
ALTER TABLE c7 MODIFY CONSTRAINT c CHECK a IN (b); -- { serverError BAD_ARGUMENTS }
-- An `IF` clause that still stores the declaration is checked like the unconditional form.
ALTER TABLE c7 ADD CONSTRAINT IF NOT EXISTS other CHECK a IN (b); -- { serverError BAD_ARGUMENTS }
ALTER TABLE c7 MODIFY CONSTRAINT IF EXISTS c CHECK a IN (b); -- { serverError BAD_ARGUMENTS }
-- Commands of one ALTER apply in order, so an `IF` clause is judged against what the preceding
-- commands leave behind rather than against the table as it was before the statement.
CREATE TABLE c9 (a UInt16, b UInt16) ENGINE = MergeTree ORDER BY a;
ALTER TABLE c9 ADD CONSTRAINT c CHECK a > 0,
    MODIFY CONSTRAINT IF EXISTS c CHECK a IN (b); -- { serverError BAD_ARGUMENTS }
CREATE TABLE c10 (a UInt16, b UInt16, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a;
ALTER TABLE c10 DROP CONSTRAINT c,
    ADD CONSTRAINT IF NOT EXISTS c CHECK a IN (b); -- { serverError BAD_ARGUMENTS }
-- Names need not be unique and DROP removes one declaration, so `c` is still there afterwards.
CREATE TABLE c11 (a UInt16, b UInt16, CONSTRAINT c CHECK a > 0, CONSTRAINT c CHECK b > 0)
    ENGINE = MergeTree ORDER BY a;
ALTER TABLE c11 DROP CONSTRAINT c,
    MODIFY CONSTRAINT IF EXISTS c CHECK a IN (b); -- { serverError BAD_ARGUMENTS }

-- A full definition under ATTACH is user input, not a definition read back from stored metadata.
ATTACH TABLE c8 UUID '5cf0a1f2-4b21-4b9f-9a3e-1d7c0e6b2a41'
    (a UInt16, b UInt16, CONSTRAINT c CHECK a IN (b))
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- A literal tuple right-hand side keeps working, and the constraint keeps being enforced.
CREATE TABLE ok1 (a UInt16, CONSTRAINT c CHECK a IN (1, 2, 3)) ENGINE = MergeTree ORDER BY a;
INSERT INTO ok1 VALUES (2);
INSERT INTO ok1 VALUES (9); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ok1;

CREATE TABLE ok2 (a UInt16, CONSTRAINT c CHECK a IN (tuple(1))) ENGINE = MergeTree ORDER BY a;
INSERT INTO ok2 VALUES (1);
SELECT count() FROM ok2;

-- A subquery right-hand side keeps working (the form 02841 relies on).
CREATE TABLE t1 (id UInt16) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (42);
CREATE TABLE ok3 (a UInt16, CONSTRAINT c CHECK a IN (SELECT id FROM t1)) ENGINE = MergeTree ORDER BY a;
INSERT INTO ok3 VALUES (42);
INSERT INTO ok3 VALUES (7); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ok3;

-- Inside a subquery the expression is SELECT-scoped, where `IN <table>` is legal shorthand.
CREATE TABLE s1 (id UInt16) ENGINE = MergeTree ORDER BY id;
INSERT INTO s1 VALUES (42);
CREATE TABLE ok4 (a UInt16, CONSTRAINT c CHECK a IN (SELECT id FROM t1 WHERE id IN s1))
    ENGINE = MergeTree ORDER BY a;
INSERT INTO ok4 VALUES (42);
INSERT INTO ok4 VALUES (7); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ok4;

-- A definition restored from stored metadata is exempt, so re-attaching keeps working.
DETACH TABLE ok1;
ATTACH TABLE ok1;
INSERT INTO ok1 VALUES (3);
SELECT count() FROM ok1;

-- Copying an accepted constraint from a source table keeps working, and stays enforced.
CREATE TABLE ok5 AS ok1;
INSERT INTO ok5 VALUES (2);
INSERT INTO ok5 VALUES (9); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ok5;
CREATE TABLE ok6 CLONE AS ok1;
INSERT INTO ok6 VALUES (9); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM ok6;

-- An `IF` clause that stores nothing is a no-op, so its expression is never persisted and the
-- command keeps succeeding.
CREATE TABLE ok7 (a UInt16, b UInt16, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a;
ALTER TABLE ok7 ADD CONSTRAINT IF NOT EXISTS c CHECK a IN (b);
ALTER TABLE ok7 MODIFY CONSTRAINT IF EXISTS absent CHECK a IN (b);
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'ok7'
    AND create_table_query LIKE '%CONSTRAINT c CHECK a > 0%' AND create_table_query NOT LIKE '%IN (%';

-- Same, for a no-op produced by an earlier command of the same ALTER.
CREATE TABLE ok8 (a UInt16, b UInt16) ENGINE = MergeTree ORDER BY a;
ALTER TABLE ok8 ADD CONSTRAINT c CHECK a > 0, ADD CONSTRAINT IF NOT EXISTS c CHECK a IN (b);
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'ok8'
    AND create_table_query LIKE '%CONSTRAINT c CHECK a > 0%' AND create_table_query NOT LIKE '%IN (%';
