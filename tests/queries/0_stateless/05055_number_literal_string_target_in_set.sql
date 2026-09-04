-- Casting a number literal from its original token text keeps a numeric target exact, but a
-- `String` target has to compare against the value the literal denotes: `1e2` stringifies to '100',
-- not to '1e2'.

SET enable_analyzer = 1;

SELECT 'analyzer' AS t;
SELECT '100' IN (tuple(materialize(1), 1e2)) AS exponent_to_string;
SELECT '100' IN (tuple(materialize(1), 100.0)) AS trailing_zero_to_string;
SELECT '1.5' IN (tuple(materialize('x'), 1.5)) AS same_text_to_string;
-- A `FixedString` target has no cast from a number at all, just as without the deferred literal.
SELECT toFixedString('100', 3) IN (tuple(materialize(toFixedString('a', 3)), 1e2)); -- { serverError NOT_IMPLEMENTED }

-- A numeric target keeps parsing the original text, which is what makes this comparison exact.
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN (tuple(materialize(CAST('9', 'Decimal128(18)')), 1.123456789012345679)) AS decimal_stays_exact;
SELECT CAST('1.5', 'Decimal128(18)') IN (tuple(materialize(CAST('9', 'Decimal128(18)')), 1.5)) AS decimal_hit;

SET enable_analyzer = 0;

SELECT 'old analyzer' AS t;
SELECT '100' IN (tuple(materialize(1), 1e2)) AS exponent_to_string;
SELECT '100' IN (tuple(materialize(1), 100.0)) AS trailing_zero_to_string;
SELECT '1.5' IN (tuple(materialize('x'), 1.5)) AS same_text_to_string;
