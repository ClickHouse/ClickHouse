-- Regression test for #90731.
-- A lone Array(T) column with a dot in its name must not be collapsed into
-- a synthetic Nested structure and must be readable as a plain array.

CREATE TABLE t1 (`a.b` Array(String)) ENGINE = Memory;
INSERT INTO t1 VALUES (['a','b','c']);
SELECT `a.b` FROM t1;

-- In a mixed table, the lone dotted column must not interfere with the
-- genuine flat-Nested group (c.x / c.y share prefix 'c').
CREATE TABLE t2 (`a.b` Array(String), `c.x` Array(Int32), `c.y` Array(String))
    ENGINE = Memory;
INSERT INTO t2 VALUES (['a','b','c'], [1,2], ['p','q']);
SELECT `a.b` FROM t2;
SELECT `c.x`, `c.y` FROM t2;

DROP TABLE t1;
DROP TABLE t2;
