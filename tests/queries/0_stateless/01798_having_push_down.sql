DROP TABLE IF EXISTS t_having;

CREATE TABLE t_having (c0 Int32, c1 UInt64) ENGINE = Memory;

INSERT INTO t_having SELECT number, number FROM numbers(1000);

SELECT sum(c0 = 0), min(c0 + 1), sum(c0 + 2) FROM t_having
GROUP BY c0 HAVING c0 = 0
SETTINGS enable_optimize_predicate_expression=0;

DROP TABLE t_having;
