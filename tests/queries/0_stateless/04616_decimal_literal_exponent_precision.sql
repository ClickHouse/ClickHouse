-- Exponent-form decimal literals compared with a Decimal column are parsed directly to Decimal
-- (exact), like plain-decimal literals, instead of through Float64. A literal that differs from the
-- stored value only beyond Float64 precision must therefore compare unequal in every spelling;
-- previously the e-notation forms fell back to Float64 and gave a false-positive match.

DROP TABLE IF EXISTS t_dec_exp;
CREATE TABLE t_dec_exp (d Decimal128(18)) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_dec_exp VALUES ('1.123456789012345678');

-- Differs in the last digit: unequal for every spelling of 1.123456789012345679.
SELECT count() FROM t_dec_exp WHERE d = 1.123456789012345679;
SELECT count() FROM t_dec_exp WHERE d = 1.123456789012345679e0;
SELECT count() FROM t_dec_exp WHERE d = 0.1123456789012345679e1;
SELECT count() FROM t_dec_exp WHERE d = 11.23456789012345679e-1;

-- The stored value: equal for every spelling of 1.123456789012345678.
SELECT count() FROM t_dec_exp WHERE d = 1.123456789012345678;
SELECT count() FROM t_dec_exp WHERE d = 1.123456789012345678e0;
SELECT count() FROM t_dec_exp WHERE d = 0.1123456789012345678e1;
SELECT count() FROM t_dec_exp WHERE d = 11.23456789012345678e-1;

DROP TABLE t_dec_exp;
