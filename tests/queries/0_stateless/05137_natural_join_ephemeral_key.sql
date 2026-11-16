DROP TABLE IF EXISTS e0;
DROP TABLE IF EXISTS e1;
DROP TABLE IF EXISTS m0;
DROP TABLE IF EXISTS p0;
DROP TABLE IF EXISTS p1;

-- `e` is `EPHEMERAL` on both `e0` and `e1`, while `m0` carries an ORDINARY column of the same name, so
-- a census that admits `EPHEMERAL` on one side only is still visible in the resulting key set.
CREATE TABLE e0 (a Int32, e Int32 EPHEMERAL, c Int32 DEFAULT e + 1) ENGINE = Memory;
CREATE TABLE e1 (a Int32, e Int32 EPHEMERAL, c Int32 DEFAULT e + 1) ENGINE = Memory;
CREATE TABLE m0 (a Int32, e Int32, c Int32) ENGINE = Memory;
CREATE TABLE p0 (x Int32, e Int32 EPHEMERAL, c0 Int32 DEFAULT e + 1) ENGINE = Memory;
CREATE TABLE p1 (y Int32, e Int32 EPHEMERAL, c1 Int32 DEFAULT e + 1) ENGINE = Memory;

INSERT INTO e0 (a, e) VALUES (1, 10), (2, 20);
INSERT INTO e1 (a, e) VALUES (1, 10), (2, 20);
INSERT INTO m0 VALUES (1, 5, 11), (2, 6, 21);
INSERT INTO p0 (x, e) VALUES (1, 10), (2, 20);
INSERT INTO p1 (y, e) VALUES (3, 30);

SET enable_analyzer = 1;

-- An `EPHEMERAL` column cannot be read, so it is not a `NATURAL JOIN` key, on either side, whether that
-- side is spelled as a table or as a table function.
SELECT * FROM e0 NATURAL JOIN e1 ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^e0$') AS m NATURAL JOIN e1 ORDER BY ALL;
SELECT * FROM m0 NATURAL JOIN e1 ORDER BY ALL;
SELECT * FROM m0 NATURAL JOIN merge(currentDatabase(), '^e1$') AS m ORDER BY ALL;
SELECT * FROM e0 NATURAL JOIN m0 ORDER BY ALL;
SELECT * FROM merge(currentDatabase(), '^e0$') AS m NATURAL JOIN m0 ORDER BY ALL;

-- `e` is the only shared name and is not a key, so there is no key at all: the join degrades to
-- `CROSS JOIN` under the existing no-common-columns rule instead of failing with `Code: 16`.
SELECT * FROM p0 NATURAL JOIN p1 ORDER BY ALL;

-- A `USING` key naming an `EPHEMERAL` column is rejected because the column is not readable, and the
-- rejection does not depend on how the left side is spelled either.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT CAST(e, 'UInt8') AS e FROM e0 JOIN e1 USING (e); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT CAST(e, 'UInt8') AS e FROM merge(currentDatabase(), '^e0$') AS m JOIN e1 USING (e); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

DROP TABLE p1;
DROP TABLE p0;
DROP TABLE m0;
DROP TABLE e1;
DROP TABLE e0;
