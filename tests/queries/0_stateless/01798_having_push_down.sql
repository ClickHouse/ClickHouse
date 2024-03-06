DROP TABLE IF EXISTS t_having;

CREATE TABLE t_having (c0 Int32, c1 UInt64) ENGINE = Memory;

INSERT INTO t_having SELECT number, number FROM numbers(1000);

SELECT sum(c0 = 0), min(c0 + 1), sum(c0 + 2) FROM t_having
GROUP BY c0 HAVING c0 = 0
SETTINGS enable_optimize_predicate_expression=0;

SELECT c0 + -1, sum(intDivOrZero(intDivOrZero(NULL, NULL), '2'), intDivOrZero(10000000000., intDivOrZero(intDivOrZero(intDivOrZero(NULL, NULL), 10), NULL))) FROM t_having GROUP BY c0 = 2, c0 = 10, intDivOrZero(intDivOrZero(intDivOrZero(NULL, NULL), NULL), NULL), c0 HAVING c0 = 2 SETTINGS enable_optimize_predicate_expression = 0;

SELECT sum(c0 + 257) FROM t_having GROUP BY c0 = -9223372036854775808, NULL, -2147483649, c0 HAVING c0 = -9223372036854775808 SETTINGS enable_optimize_predicate_expression = 0;

SET enable_positional_arguments=0;
SELECT c0 + -2, c0 + -9223372036854775807, c0 = NULL FROM t_having GROUP BY c0 = 0.9998999834060669, 1023, c0 HAVING c0 = 0.9998999834060669 SETTINGS enable_optimize_predicate_expression = 0;

DROP TABLE t_having;
