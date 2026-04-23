-- Regression test for STID 6488-5f6c:
-- `extractKeyValuePairs` raised a `LOGICAL_ERROR` exception ("Failed to handle
-- unexpected quoting character. This is a bug") when called with
-- `quoting_character = '\0'` (NUL byte) and strategy = `accept`.
--
-- Root cause was in the SSE2 runtime path of `find_first_symbols_or_null` in
-- `base/find_symbols.h`: `mm_is_in_prepare` zero-initialised a fixed 16-slot
-- needle array, and `mm_is_in_execute` iterated all 16 slots unconditionally,
-- so any NUL byte in the haystack matched the unused zero-padded slots even
-- when the caller's needle set did not include NUL. With
-- `quoting_character = '\0'` and strategy = `accept`, `NeedleFactory` omits NUL
-- from the needle set (since the character should be treated as data), but the
-- SIMD still reported a match on NUL in the haystack. The state handler then
-- entered the quoting-character branch for the `ACCEPT` strategy, which was
-- coded as unreachable and threw `LOGICAL_ERROR`.
--
-- The fix makes `mm_is_in_execute` respect `num_chars`, so zero-padded slots no
-- longer participate in the compare. Zero is just an ordinary byte value; NUL
-- is now a first-class quoting / delimiter character like any other.

-- The exact fuzzer repro: needs >= 16 bytes of data with a NUL byte in the
-- first 16 bytes so the SIMD block is exercised.
-- Before the fix: server raised a `LOGICAL_ERROR` exception.
-- After the fix: NUL is ignored for the needle search, so `extractKeyValuePairs`
-- finds no `:` or `,` in the input and returns an empty map.
SELECT extractKeyValuePairs(toFixedString('x\0yyyyyyyyyyyyyyyy', 18), ':', ',', '\0', 'accept');

-- NUL as a quoting character should now work correctly for all three strategies.
-- `accept`: quoting character is treated as ordinary data, so a literal NUL in the
-- input becomes part of the key or value.
SELECT extractKeyValuePairs('a:1,b:2', ':', ',', '\0', 'accept');

-- `invalid`: an unexpected NUL in the key-reading state invalidates the pair.
-- Here the input has no NUL, so parsing is unaffected.
SELECT extractKeyValuePairs('a:1,b:2', ':', ',', '\0', 'invalid');

-- `promote`: an unexpected NUL promotes to quoted-value reading. With no NUL in
-- the input, this is indistinguishable from the default pass-through.
SELECT extractKeyValuePairs('a:1,b:2', ':', ',', '\0', 'promote');

-- NUL-delimited quoted value: `promote` strategy promotes the NUL into quoted-reading,
-- reads until the closing NUL.
SELECT extractKeyValuePairs('k:\0hello world\0', ':', ',', '\0', 'promote');

-- Non-NUL quoting characters must still work, including with NUL bytes in the data.
SELECT extractKeyValuePairs('a:1,b:2', ':', ',', '"', 'accept');
SELECT extractKeyValuePairs('"k":"v"', ':', ',', '"', 'promote');
