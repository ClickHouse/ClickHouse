-- aggregate functions are JIT-compiled after 3 queries
-- this tests checks that 4-th query still works with optimization https://github.com/ClickHouse/ClickHouse/issues/72610
DROP TABLE IF EXISTS t;
CREATE TABLE t(a Int64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3), (4), (5), (6);
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
