-- Tags: no-parallel, no-fasttest
DROP TABLE IF EXISTS rollup;

CREATE TABLE rollup(a Nullable(Int64), b Nullable(Int64), c Nullable(Int64)) engine=Memory;
INSERT INTO rollup SELECT 1, 9, 1;
INSERT INTO rollup SELECT 1, 11, 1;

SELECT a, toString(b)||'' as x, c, count() FROM rollup GROUP BY a,x,c WITH rollup ORDER BY a,x,c;

SELECT a, toString(b) as x, c, count() from rollup GROUP BY  a,x,c WITH rollup ORDER BY a,x,c;

SELECT a, b as x, c, count() from rollup GROUP BY  c,x,a WITH rollup ORDER BY a,x,c;

SELECT a, toString(b) as x, c, count() from rollup GROUP BY  c,x,a WITH rollup ORDER BY a,x,c;

DROP TABLE rollup;
