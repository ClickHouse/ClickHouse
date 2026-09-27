-- Regression test for use-of-uninitialized-value in `StringSearcher<false, false>`
-- (UTF-8 case-insensitive) when the needle contains an invalid UTF-8 sequence.
--
-- Reported by MemorySanitizer at `src/Common/StringSearcher.h:563:61` from
-- `Volnitsky::Volnitsky` -> `MatchImpl::vectorVector` -> `FunctionLike` (`ilike`)
-- across many unrelated PRs (chronic noise on `Unit tests (msan, function_prop_fuzzer)`).
--
-- Root cause:
--   In the SIMD-cache loop in the `UTF8CaseInsensitiveStringSearcher` constructor,
--   when `convertUTF8ToCodePoint` returned an empty optional for an invalid byte
--   sequence in the middle of the needle, the inner loop still inserted
--   `l_seq[j]` / `u_seq[j]` into `cachel` / `cacheu` even though those buffers
--   had not been written for this iteration. Symmetrically, the first-character
--   invalid-UTF-8 branch above the loop copied `seqLength(*needle)` bytes from
--   the needle without clamping to `needle_size`, so a truncated first sequence
--   (e.g. a 1-byte needle starting with `\xE4`, where `seqLength` is 3) would
--   read past `needle_end` into uninitialized memory and propagate sanitizer
--   noise from the same family.
--
-- The fix mirrors the existing verbatim-bytes handling used for the first
-- character: when the conversion fails, the bytes of the needle are copied
-- into `l_seq` / `u_seq` directly, with the source length clamped to
-- `needle_size` (first character) or `needle_end - needle_pos` (inner loop).

-- A 3-byte needle: 'a' (valid ASCII), followed by `\xC3\x28` (invalid UTF-8 -
-- `\xC3` is a 2-byte start byte, but `\x28` is not a continuation byte).
-- `seqLength(\xC3) = 2` and `needle_end - needle_pos = 2` on the second
-- iteration, so `src_len = 2`. Without the fix this reads uninitialized
-- `l_seq[1]` / `u_seq[1]`.
SELECT '-- ilike (Volnitsky -> UTF8CaseInsensitiveStringSearcher)';
SELECT count() FROM (SELECT ilike('some long haystack that is much longer than sixteen bytes', concat('%', unhex('61C328'), '%')));

SELECT '-- positionCaseInsensitiveUTF8';
SELECT positionCaseInsensitiveUTF8('some long haystack that is much longer than sixteen bytes', unhex('61C328'));

SELECT '-- multiSearchAnyCaseInsensitiveUTF8';
SELECT multiSearchAnyCaseInsensitiveUTF8('some long haystack that is much longer than sixteen bytes', [unhex('61C328')]);

SELECT '-- startsWithCaseInsensitiveUTF8';
SELECT startsWithCaseInsensitiveUTF8('some long haystack that is much longer than sixteen bytes', unhex('61C328'));

SELECT '-- endsWithCaseInsensitiveUTF8';
SELECT endsWithCaseInsensitiveUTF8('some long haystack that is much longer than sixteen bytes', unhex('61C328'));

-- A trickier case: valid non-ASCII first character followed by invalid UTF-8 mid-needle.
-- `\xC3\xA4` is `ä` (valid 2-byte UTF-8). Then `\xE4\x28\x29` is an invalid 3-byte
-- start (`\xE4` expects two continuation bytes, but `\x28` is ASCII '(').
SELECT '-- ilike with mixed valid/invalid UTF-8 needle';
SELECT count() FROM (SELECT ilike('a long enough Bär haystack and some trailing bytes', concat('%', unhex('C3A4E42829'), '%')));

-- Multi-row variant: makes the SIMD path more likely to engage on the cached
-- needle across many haystacks.
SELECT '-- multi-row ilike';
SELECT count() FROM
(
    SELECT ilike(materialize('some long haystack that is much longer than sixteen bytes'), concat('%', unhex('61C328'), '%'))
    FROM numbers(64)
);

-- Truncated first-character invalid-UTF-8 needle: a 1-byte needle starting with
-- `\xE4`. `seqLength(\xE4) = 3` but `needle_size = 1`, so without the clamp on
-- `src_len` in the first-character branch the constructor would memcpy 3 bytes
-- from a 1-byte needle, reading past `needle_end` into uninitialized memory.
SELECT '-- ilike with 1-byte truncated UTF-8 needle';
SELECT count() FROM (SELECT ilike('a long enough haystack that easily exceeds sixteen bytes', concat('%', unhex('E4'), '%')));

SELECT '-- positionCaseInsensitiveUTF8 with 1-byte truncated UTF-8 needle';
SELECT positionCaseInsensitiveUTF8('a long enough haystack that easily exceeds sixteen bytes', unhex('E4'));

SELECT '-- multiSearchAnyCaseInsensitiveUTF8 with 1-byte truncated UTF-8 needle';
SELECT multiSearchAnyCaseInsensitiveUTF8('a long enough haystack that easily exceeds sixteen bytes', [unhex('E4')]);

SELECT '-- startsWithCaseInsensitiveUTF8 with 1-byte truncated UTF-8 needle';
SELECT startsWithCaseInsensitiveUTF8('a long enough haystack that easily exceeds sixteen bytes', unhex('E4'));

SELECT '-- endsWithCaseInsensitiveUTF8 with 1-byte truncated UTF-8 needle';
SELECT endsWithCaseInsensitiveUTF8('a long enough haystack that easily exceeds sixteen bytes', unhex('E4'));

-- Multi-row variant for the truncated first-character case.
SELECT '-- multi-row ilike with 1-byte truncated UTF-8 needle';
SELECT count() FROM
(
    SELECT ilike(materialize('a long enough haystack that easily exceeds sixteen bytes'), concat('%', unhex('E4'), '%'))
    FROM numbers(64)
);
