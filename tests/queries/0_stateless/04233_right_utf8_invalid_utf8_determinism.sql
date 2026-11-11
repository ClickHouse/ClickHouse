-- `rightUTF8(s, -k)` had inconsistent results between the constant-length path and the
-- per-row dynamic-length path when `s` contained invalid UTF-8 bytes.
-- `countCodePoints` skips lone continuation bytes (0x80..0xBF that aren't preceded by a
-- leading byte) but `skipCodePointsForward/Backward` and `seqLength` treat them as a
-- 1-byte code point. The const path goes through `getSliceFromLeft(offset)` which is
-- internally consistent; the dynamic path used `getSliceFromRight(elemSize - k,
-- elemSize - k)` which mixed the two views and dropped trailing bytes.

-- Test string: 'aaa' || 0xA0 || 'bbb' (7 bytes, lengthUTF8 = 6 because 0xA0 is a lone
-- continuation byte). All three forms below must give the same result: drop the first
-- code point, keep the rest (6 bytes: 'aa' || 0xA0 || 'bbb').
WITH 'aaa' || char(0xA0) || 'bbb' AS s
SELECT
    hex(rightUTF8(s, toInt8(-1))) AS pure_const,
    hex(rightUTF8(materialize(s), toInt8(-1))) AS const_len,
    hex(rightUTF8(materialize(s), materialize(toInt8(-1)))) AS dynamic_len
FORMAT Vertical;

-- Same expectation for a multi-row column with the lone-continuation string.
SELECT n, hex(rightUTF8(s, l)) FROM (
    SELECT 0 AS n, 'aaa' || char(0xA0) || 'bbb' AS s, toInt8(-1) AS l
    UNION ALL
    SELECT 1, 'xyz' || char(0xA0) || 'wv', toInt8(-2)
    UNION ALL
    SELECT 2, char(0xA0) || 'abc', toInt8(-1)
) ORDER BY n;

-- Sanity: positive length still works.
WITH 'aaa' || char(0xA0) || 'bbb' AS s
SELECT
    hex(rightUTF8(s, toInt8(3))) AS pure_const_pos,
    hex(rightUTF8(materialize(s), materialize(toInt8(3)))) AS dynamic_len_pos
FORMAT Vertical;
