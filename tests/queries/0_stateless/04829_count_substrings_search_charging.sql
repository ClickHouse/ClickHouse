-- Tags: no-fasttest
-- no-fasttest: case-insensitive UTF-8 folding uses ICU.
-- The searchers now report the work they do so a caller can observe a deadline inside one search.
-- Charging runs on the matching path too, so it must not drop, duplicate or misplace an occurrence.

-- All three functions, all dispatch branches: constant/vector needle against constant/vector haystack.
SELECT countSubstrings(materialize('abcabcabc'), 'abc'), countSubstringsCaseInsensitive(materialize('AbCabc'), 'abc'), countSubstringsCaseInsensitiveUTF8(materialize('ПРИВЕТпривет'), 'привет');
SELECT countSubstrings(materialize('aaaa'), materialize('aa')), countSubstringsCaseInsensitive(materialize('XyXy'), materialize('xy')), countSubstringsCaseInsensitiveUTF8(materialize('ЁЖЁж'), materialize('ёж'));
SELECT countSubstrings('abcabcabc', materialize('abc')), countSubstringsCaseInsensitive('AbCabc', materialize('abc')), countSubstringsCaseInsensitiveUTF8('ПРИВЕТпривет', materialize('привет'));

-- A start position, which restarts the scan mid-haystack.
SELECT countSubstrings(materialize('abcabcabc'), 'abc', 4), countSubstrings('abcabcabc', materialize('abc'), 4), countSubstrings(materialize('abcabcabc'), materialize('abc'), materialize(toUInt64(7)));

-- Empty haystack and empty needle, where the loop is entered zero times or returns immediately.
SELECT countSubstrings(materialize(''), 'a'), countSubstrings(materialize('abc'), ''), countSubstrings('', materialize('a')), countSubstrings('abc', materialize(''));

-- Folding across code points whose lowercase differs in byte length (U+212A KELVIN SIGN vs 'k'): a match
-- can span more bytes than the needle, so charging must not be mistaken for progress past the match.
SELECT countSubstringsCaseInsensitiveUTF8(materialize('a' || repeat('\xE2\x84\xAA', 8)), materialize('a' || repeat('k', 8)));
SELECT countSubstringsCaseInsensitiveUTF8(materialize('a' || repeat('K', 8)), materialize('a' || repeat('\xE2\x84\xAA', 8)));
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\xE2\x84\xAA', 20)), materialize(repeat('k', 5)));

-- A run of continuation bytes long enough for the vector loop to skip a whole window, which this change
-- realigns from within a bound. A match must still be found right after the run: stopping the realignment
-- mid-run is only safe because a continuation byte begins no match. The case-sensitive count is asserted
-- alongside, as an oracle that does not go through this searcher.
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\x80', 64) || 'Ж'), 'Ж'), countSubstrings(materialize(repeat('\x80', 64) || 'Ж'), 'Ж');
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\x80', 64) || 'a'), 'a'), countSubstrings(materialize(repeat('\x80', 64) || 'a'), 'a');
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\x80', 64) || repeat('Ж', 3)), repeat('Ж', 3)), countSubstrings(materialize(repeat('\x80', 64) || repeat('Ж', 3)), repeat('Ж', 3));
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat('\x80', 100) || 'Ж' || repeat('\x80', 100) || 'Ж'), 'Ж'), countSubstrings(materialize(repeat('\x80', 100) || 'Ж' || repeat('\x80', 100) || 'Ж'), 'Ж');
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat(repeat('\x80', 40) || 'Ж', 10)), 'Ж'), countSubstrings(materialize(repeat(repeat('\x80', 40) || 'Ж', 10)), 'Ж');

-- Enough occurrences that the charge crosses its reporting threshold many times within one search.
SELECT countSubstringsCaseInsensitiveUTF8(materialize(repeat(repeat('Ж', 200) || 'Щ', 100000)), repeat('Ж', 16) || 'Щ') SETTINGS max_threads = 1;
