-- `lcm` returned a negative result when exactly one argument was negative and the computation ran in a
-- wide integer type: the sign branch of its `abs` helper was selected with the `std` trait, which is
-- `false` for `Int128`/`Int256`, so the negative value passed through and the unsigned conversion of the
-- negative quotient wrapped. With both arguments negative the two wraps cancelled, which is what the
-- existing tests covered.

SELECT 'one negative argument';
SELECT lcm(toInt128(-6), toInt128(4)), lcm(toInt128(6), toInt128(-4));
SELECT lcm(toInt256(-6), toInt256(4)), lcm(toInt256(6), toInt256(-4));
SELECT lcm(toInt128(-6), toInt8(4)), lcm(toInt8(-6), toInt128(4));
SELECT lcm(toInt64(-6), toInt64(4)), lcm(toInt32(-6), toInt32(4));

SELECT 'both negative, and both positive';
SELECT lcm(toInt128(-6), toInt128(-4)), lcm(toInt128(6), toInt128(4));
SELECT lcm(toInt256(-6), toInt256(-4)), lcm(toInt256(6), toInt256(4));

SELECT 'gcd, which was never affected';
SELECT gcd(toInt128(-6), toInt128(4)), gcd(toInt256(-6), toInt256(4));

SELECT 'over a column, not a constant';
DROP TABLE IF EXISTS t_lcm_wide;
CREATE TABLE t_lcm_wide (a Int128, b Int128) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lcm_wide VALUES (-6, 4), (6, -4), (-6, -4), (6, 4);
SELECT a, b, lcm(a, b) FROM t_lcm_wide ORDER BY a, b;
DROP TABLE t_lcm_wide;
