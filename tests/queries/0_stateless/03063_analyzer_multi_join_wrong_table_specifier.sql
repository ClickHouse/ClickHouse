-- https://github.com/ClickHouse/ClickHouse/issues/23162
SET enable_analyzer=1;
CREATE TABLE t1 ( k Int64, x Int64) ENGINE = Memory;

CREATE TABLE t2( x Int64 ) ENGINE = Memory;

create table s (k Int64, d DateTime)  Engine=Memory;

SELECT * FROM t1
INNER JOIN s ON t1.k = s.k
INNER JOIN t2 ON t2.x = t1.x
WHERE (t1.d >= now()); -- { serverError UNKNOWN_IDENTIFIER }

SELECT * FROM t1
INNER JOIN s ON t1.k = s.k
WHERE (t1.d >= now()); -- { serverError UNKNOWN_IDENTIFIER }
