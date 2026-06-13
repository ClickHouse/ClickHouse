-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (1 = ANY(SELECT 1)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 IN (SELECT 1)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PARTITION BY (1 IN (SELECT 1)) ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY c0 PRIMARY KEY (c0 IN (SELECT 1)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int, INDEX idx (c0 IN (SELECT 1)) TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int, d DateTime) ENGINE = MergeTree() ORDER BY c0 TTL d + INTERVAL (1 IN (SELECT 1)) DAY; -- { serverError BAD_ARGUMENTS }

-- The right-hand side of `IN` may also be a table reference (an identifier), which used to slip past the
-- subquery check above and then abort with "Not-ready Set" on the first INSERT. Forbid it in key expressions too.
DROP TABLE IF EXISTS tref;
CREATE TABLE tref (c0 Int) ENGINE = Memory;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 IN tref); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 GLOBAL IN tref); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 NOT IN tref); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PARTITION BY (c0 IN tref) ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY c0 PRIMARY KEY (c0 IN tref); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int, INDEX idx (c0 IN tref) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY c0; -- { serverError BAD_ARGUMENTS }

-- A literal set on the right-hand side of `IN` is a constant and remains allowed in a key expression.
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 IN (1, 2, 3));
INSERT INTO t0 VALUES (1), (5);
SELECT count() FROM t0;
DROP TABLE t0;
DROP TABLE tref;
