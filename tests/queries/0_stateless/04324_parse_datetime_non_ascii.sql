-- Regression test: parseDateTime with non-ASCII bytes in input/format must not cause
-- a logical error (std::length_error) from String(char, 1) where a high-bit char
-- is implicitly converted to a huge size_t count.

SELECT parseDateTime(CAST('\xaed\xc6\xefwvA\xe6' AS String), CAST('%r2f\x820' AS String)); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('\x80', '%H'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('12:\xff', '%H:%i'); -- { serverError CANNOT_PARSE_DATETIME }
