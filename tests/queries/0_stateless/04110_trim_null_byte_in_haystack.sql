-- Companion regression test for the SIMD fix in `base/find_symbols.h`.
-- See also: `04109_extract_key_value_pairs_null_quoting.sql` and
-- `src/Common/tests/gtest_find_symbols.cpp::FindSymbols.NullByteInHaystack`.
--
-- Pre-fix behaviour: `trim` with a custom `trim_characters` argument silently
-- treated embedded NUL bytes as if they were part of the trim set. The
-- underlying defect was in `mm_is_in_execute` iterating all 16 zero-padded
-- needle slots, so any NUL in the haystack matched an unused slot regardless
-- of the user-supplied needle. `trim` exercises the SSE2 runtime-needle path
-- via `find_first_not_symbols` and `find_last_not_symbols_or_null`
-- (the latter has no SSE4.2 variant, so every needle size hits the buggy
-- primitive). This is a silent wrong-result bug — never raises an exception,
-- so the AST fuzzer could not surface it; it was found while auditing sibling
-- callers for the `extractKeyValuePairs` fix.
--
-- Each haystack below is >= 16 bytes so the SSE2 16-byte block is exercised
-- (the SIMD loop only runs when `pos + 15 < end`).

-- `trimLeft`: a leading NUL byte is NOT in the needle set, so nothing should
-- be trimmed. Pre-fix, the NUL was treated as an `x`, so the whole string was
-- consumed and the result was empty.
SELECT length(trimLeft(char(0) || 'xxxxxxxxxxxxxxx', 'x')) = 16;

-- `trimRight`: a trailing NUL byte is NOT in the needle set, so nothing
-- should be trimmed. Pre-fix, the NUL was treated as an `x`, so the whole
-- string was consumed from the right and the result was empty.
SELECT length(trimRight('xxxxxxxxxxxxxxx' || char(0), 'x')) = 16;

-- `trimBoth`: NUL on both ends must be preserved. Needle `x` only.
SELECT length(trimBoth(char(0) || repeat('x', 15) || char(0), 'x')) = 17;

-- Control: a non-zero control byte (SOH, 0x01) is correctly preserved. This
-- demonstrates that the bug is specific to NUL, not to control characters in
-- general, and serves as a sanity check that the test itself detects the
-- difference correctly.
SELECT length(trimLeft(char(1) || 'xxxxxxxxxxxxxxx', 'x')) = 16;

-- `FixedString`: shorter values are zero-padded in the column storage, so the
-- haystack seen by `trim` contains real NUL bytes. With needle `x`, none of
-- those NUL bytes are in the needle set and the padded tail must be preserved.
SELECT length(trimLeft(CAST('abc', 'FixedString(16)'), 'x')) = 16;

-- A NUL that IS explicitly in the needle set must still match (backward
-- compatibility — NUL is now a first-class needle character). The needle
-- `char(0)` contains only NUL, so the leading NUL in the haystack is a valid
-- trim target and the result keeps only the `x` tail.
SELECT length(trimLeft(char(0) || 'xxxxxxxxxxxxxxx', char(0))) = 15;
