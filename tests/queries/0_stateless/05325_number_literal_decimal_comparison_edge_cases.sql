-- A literal that needs more than 76 digits does not fit Decimal256: it compares with a Decimal
-- through Float64 in every spelling, the integer one included.
SELECT CAST('0', 'Decimal64(0)') = 12345678901234567890123456789012345678901234567890123456789012345678901234567;
SELECT CAST('0', 'Decimal64(0)') < 12345678901234567890123456789012345678901234567890123456789012345678901234567;
SELECT CAST('0', 'Decimal64(0)') = 1.2345678901234567890123456789012345678901234567890123456789012345678901234567e76;
SELECT CAST('0', 'Decimal64(0)') < 1.2345678901234567890123456789012345678901234567890123456789012345678901234567e76;

-- A literal with a finer scale than a narrow Decimal still compares without overflow.
SELECT CAST('999999999', 'Decimal32(0)') = 0.5;
SELECT CAST('999999999', 'Decimal32(0)') < 0.5;
SELECT CAST('999999999', 'Decimal32(0)') IN (0.5, 1.5);

DROP TABLE IF EXISTS t_mixed_key;
CREATE TABLE t_mixed_key (s String, d Decimal128(18)) ENGINE = Memory;
INSERT INTO t_mixed_key VALUES ('100', '1.123456789012345728');

-- The literal differs from the stored value only beyond Float64 precision, so no form matches,
-- including a tuple whose String key is compared with a number.
SELECT
    countIf(d = 1.123456789012345679e0) AS eq,
    countIf(d IN (1.123456789012345679e0)) AS in_scalar,
    countIf((100, d) IN ((100, 1.123456789012345679e0))) AS in_tuple_numeric,
    countIf((s, d) IN (('100', 1.123456789012345679e0))) AS in_tuple_string_key,
    countIf((s, d) IN ((100, 1.123456789012345679e0))) AS in_tuple_mixed_key,
    countIf((s, d) IN ((100, 1.123456789012345679e0), (200, 2.5))) AS in_tuple_mixed_key_list
FROM t_mixed_key
FORMAT Vertical;

-- The stored value matches in every form.
SELECT
    countIf(d = 1.123456789012345728e0) AS eq,
    countIf((s, d) IN ((100, 1.123456789012345728e0))) AS in_tuple_mixed_key,
    countIf((s, d) IN ((100, 1.123456789012345728e0), (200, 2.5))) AS in_tuple_mixed_key_list
FROM t_mixed_key
FORMAT Vertical;

DROP TABLE t_mixed_key;
