SET optimize_move_to_prewhere = 1; -- works only for PREWHERE

CREATE TABLE t1 (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = Memory;
INSERT INTO t1 SELECT number, number * 10, number * 100, number * 1000 FROM numbers(1000000);

EXPLAIN SYNTAX
SELECT * FROM t1
WHERE (a, b) = (1, 2) AND (c, d, a) = (3, 4, 5) OR (a, b, 1000) = (c, 10, d) OR ((a, b), 1000) = ((c, 10), d);
