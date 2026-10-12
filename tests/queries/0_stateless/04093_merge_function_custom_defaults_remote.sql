-- Tags: no-fasttest
-- Test that merge() function works correctly with custom DEFAULT expressions
-- for columns missing in some underlying remote/distributed tables.
-- Uses the data type's zero default, consistent with the local MergeTree path.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS r_t1;
DROP TABLE IF EXISTS r_t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- Table t1 has column b with a custom DEFAULT value of 42.
-- When t2 is missing column b, the merge() function uses the type default (0),
-- consistent with the local path behavior in addMissingDefaults.
CREATE TABLE t1 (a Int64, b Int64 DEFAULT 42) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a Int64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 10);
INSERT INTO t2 VALUES (2);

-- Local: missing column b in t2 gets the type default (0).
SELECT a, b FROM merge(currentDatabase(), 't[12]') ORDER BY a;

-- Remote: same behavior expected.
CREATE TABLE r_t1 AS remote('127.0.0.1', currentDatabase(), 't1');
CREATE TABLE r_t2 AS remote('127.0.0.1', currentDatabase(), 't2');

SELECT a, b FROM merge(currentDatabase(), 'r_t[12]') ORDER BY a;

DROP TABLE r_t1;
DROP TABLE r_t2;
DROP TABLE t1;
DROP TABLE t2;
