-- A decimal literal compared with a Decimal column is normalized (exponent folded, insignificant
-- trailing zeroes dropped) and parsed exactly into Decimal, so extra trailing zeroes or exponent
-- notation don't push it onto the Float64 path and reintroduce a false-positive match beyond Float64
-- precision.

DROP TABLE IF EXISTS t_dec_tz;
CREATE TABLE t_dec_tz (d Decimal128(18)) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_dec_tz VALUES ('1.123456789012345678');

-- Differs in the last significant digit; trailing zeroes / exponent must not turn it into a match.
SELECT count() FROM t_dec_tz WHERE d = 1.1234567890123456790000000000000000000000000000000000000000000000000000000000000;
SELECT count() FROM t_dec_tz WHERE d = 1.123456789012345679e0;

-- The exact stored value, spelled with trailing zeroes or exponent, still matches.
SELECT count() FROM t_dec_tz WHERE d = 1.1234567890123456780000000000000000000000000000000000000000000000000000000000000;
SELECT count() FROM t_dec_tz WHERE d = 1.123456789012345678e0;

DROP TABLE t_dec_tz;
