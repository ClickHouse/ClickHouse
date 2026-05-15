-- Regression test: comparison of constant FixedString with materialized String
-- must use zero-padded semantics (trailing \0 in FixedString is padding, not data).
-- Previously, the const-FixedString-vs-String-vector code path used plain memcmp
-- instead of zero-padded memcmp, giving wrong results.

-- All four const/materialized combinations must agree.

SELECT '-- greater';
SELECT greater(CAST('\0\0\0\0' AS FixedString(4)), CAST('' AS String));
SELECT greater(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String)));
SELECT greater(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String));
SELECT greater(materialize(CAST('\0\0\0\0' AS FixedString(4))), materialize(CAST('' AS String)));

SELECT '-- less';
SELECT less(CAST('\0\0\0\0' AS FixedString(4)), CAST('' AS String));
SELECT less(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String)));
SELECT less(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String));
SELECT less(materialize(CAST('\0\0\0\0' AS FixedString(4))), materialize(CAST('' AS String)));

SELECT '-- equals';
SELECT equals(CAST('abc' AS FixedString(6)), CAST('abc' AS String));
SELECT equals(CAST('abc' AS FixedString(6)), materialize(CAST('abc' AS String)));
SELECT equals(materialize(CAST('abc' AS FixedString(6))), CAST('abc' AS String));
SELECT equals(materialize(CAST('abc' AS FixedString(6))), materialize(CAST('abc' AS String)));

SELECT '-- notEquals';
SELECT notEquals(CAST('abc' AS FixedString(6)), CAST('abc' AS String));
SELECT notEquals(CAST('abc' AS FixedString(6)), materialize(CAST('abc' AS String)));
SELECT notEquals(materialize(CAST('abc' AS FixedString(6))), CAST('abc' AS String));
SELECT notEquals(materialize(CAST('abc' AS FixedString(6))), materialize(CAST('abc' AS String)));

SELECT '-- greaterOrEquals (padding should be equal)';
SELECT greaterOrEquals(CAST('\0\0\0\0' AS FixedString(4)), CAST('' AS String));
SELECT greaterOrEquals(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String)));
SELECT greaterOrEquals(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String));
SELECT greaterOrEquals(materialize(CAST('\0\0\0\0' AS FixedString(4))), materialize(CAST('' AS String)));

SELECT '-- lessOrEquals (padding should be equal)';
SELECT lessOrEquals(CAST('\0\0\0\0' AS FixedString(4)), CAST('' AS String));
SELECT lessOrEquals(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String)));
SELECT lessOrEquals(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String));
SELECT lessOrEquals(materialize(CAST('\0\0\0\0' AS FixedString(4))), materialize(CAST('' AS String)));

SELECT '-- Nullable wrapper (original bug report)';
SELECT greater(CAST('\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0' AS Nullable(FixedString(26))), CAST('' AS String));
SELECT greater(CAST('\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0' AS Nullable(FixedString(26))), materialize(CAST('' AS String)));

SELECT '-- Real differences still detected';
SELECT greater(CAST('xyz' AS FixedString(6)), materialize(CAST('abc' AS String)));
SELECT less(CAST('abc' AS FixedString(6)), materialize(CAST('xyz' AS String)));
SELECT greater(materialize(CAST('hello' AS String)), CAST('hell' AS FixedString(4)));
