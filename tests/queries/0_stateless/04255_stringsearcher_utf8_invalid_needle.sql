-- Regression test for use-of-uninitialized-value in `StringSearcher<false, false>`
-- (UTF-8 case-insensitive) when the needle contains an invalid UTF-8 sequence
-- in the middle (i.e. not as the first character).
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
--   had not been written for this iteration.
--
-- The fix mirrors the existing verbatim-bytes handling used for the first
-- character: when the conversion fails, the bytes of the needle are copied
-- into `l_seq` / `u_seq` directly.

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
