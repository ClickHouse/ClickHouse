-- Regression test for the case-insensitive UTF-8 searcher's empty-needle handling.
-- `startsWithCaseInsensitiveUTF8` / `endsWithCaseInsensitiveUTF8` call the searcher's `compare`
-- directly. An empty needle must match (return 1); without the empty-needle guard `compare`
-- would dereference a pointer at the end of the haystack.

SELECT '-- empty needle matches';
SELECT startsWithCaseInsensitiveUTF8('abc', '');
SELECT endsWithCaseInsensitiveUTF8('abc', '');

SELECT '-- empty needle and empty haystack';
SELECT startsWithCaseInsensitiveUTF8('', '');
SELECT endsWithCaseInsensitiveUTF8('', '');

SELECT '-- empty needle with a multi-byte UTF-8 haystack';
SELECT startsWithCaseInsensitiveUTF8('Ёжик', '');
SELECT endsWithCaseInsensitiveUTF8('Ёжик', '');

SELECT '-- non-empty needle is unaffected';
SELECT startsWithCaseInsensitiveUTF8('abc', 'AB');
SELECT endsWithCaseInsensitiveUTF8('abc', 'BC');
SELECT startsWithCaseInsensitiveUTF8('abc', 'xy');
