-- Regression test for the AVX2/NEON case-insensitive UTF-8 search path.
-- An earlier revision masked out first-byte candidates below 0x40 (ASCII digits, space,
-- '%', '/', NUL, ...) as if they were UTF-8 continuation bytes, so a needle whose first
-- byte is below 0x40 was skipped in haystacks longer than the SIMD window and the match
-- was missed. The match here is placed past byte 64 so the vectorized scan is exercised.

SELECT '-- positionCaseInsensitiveUTF8, needle first byte below 0x40';
SELECT positionCaseInsensitiveUTF8(repeat('a', 80) || '1bcd', '1bcd');
SELECT positionCaseInsensitiveUTF8(repeat('a', 80) || ' xyz', ' xyz');
SELECT positionCaseInsensitiveUTF8(repeat('a', 80) || '/path', '/path');
SELECT positionCaseInsensitiveUTF8(repeat('a', 80) || '%val', '%val');

SELECT '-- the matched substring also folds case';
SELECT positionCaseInsensitiveUTF8(repeat('a', 80) || '1BCD', '1bcd');

SELECT '-- ILIKE with a low-byte needle in a long haystack';
SELECT repeat('a', 80) || '1bcd' ILIKE '%1bcd%';

SELECT '-- multiSearchAnyCaseInsensitiveUTF8 with a low-byte needle';
SELECT multiSearchAnyCaseInsensitiveUTF8(repeat('a', 80) || '1bcd', ['1bcd']);

SELECT '-- absent low-byte needle is still not found';
SELECT positionCaseInsensitiveUTF8(repeat('a', 80) || 'tail', '1bcd');
