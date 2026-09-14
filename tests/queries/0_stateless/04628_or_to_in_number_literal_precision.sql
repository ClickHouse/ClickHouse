-- The old analyzer rewrites `x = a OR x = b OR x = c` into `x IN (a, b, c)`. Decimal, exponent and
-- wide-integer literals are now deferred (NumberLiteral) which has no ordering of its own, so the
-- rewrite must not sort raw NumberLiterals (Field::operator< on NumberLiteral used to throw). It
-- orders by the resolved value and keeps the exact literal, so the resulting IN is still correct.

SET optimize_min_equality_disjunction_chain_length = 3;

-- Decimal-point and exponent literals in the chain (Float64-exact values keep the result independent
-- of the separate Float64/Decimal set-comparison behaviour).
DROP TABLE IF EXISTS t_or_in;
CREATE TABLE t_or_in (x Float64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_or_in VALUES (1.5), (2.5), (3.5);
SELECT count() FROM t_or_in WHERE x = 1.5 OR x = 2.5 OR x = 3.5 SETTINGS enable_analyzer = 0;
SELECT count() FROM t_or_in WHERE x = 1.5e0 OR x = 25e-1 OR x = 0.35e1 SETTINGS enable_analyzer = 0;
SELECT count() FROM t_or_in WHERE x = 1.5 OR x = 2.5 OR x = 9.5 SETTINGS enable_analyzer = 0;
DROP TABLE t_or_in;

-- Wide-integer OR chains stay as NumberLiteral too and are compared exactly.
DROP TABLE IF EXISTS t_or_in_big;
CREATE TABLE t_or_in_big (x UInt256) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_or_in_big VALUES (100000000000000000000000), (200000000000000000000000);
SELECT count() FROM t_or_in_big
    WHERE x = 100000000000000000000000 OR x = 200000000000000000000000 OR x = 300000000000000000000000
    SETTINGS enable_analyzer = 0;
DROP TABLE t_or_in_big;
