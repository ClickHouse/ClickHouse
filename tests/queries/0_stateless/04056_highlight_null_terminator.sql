-- Test: highlight() edge cases — empty strings, FixedString conversion, adjacent rows.

SET enable_analyzer = 1;

-- Empty string: should return empty
SELECT '-- empty string';
SELECT highlight('', ['a']);
SELECT length(highlight('', ['a']));

-- Single-char match
SELECT '-- single char';
SELECT highlight('a', ['a']);
SELECT length(highlight('a', ['a'])) = length('<em>a</em>');

-- Adjacent rows: verify no cross-row contamination
SELECT '-- adjacent rows';
SELECT highlight(s, ['b']) FROM (SELECT arrayJoin(['a', 'b', 'c']) AS s);

-- FixedString empty after stripping: padding is all zeros
SELECT '-- FixedString all-zero padding';
SELECT highlight(toFixedString('', 4), ['a']);
SELECT length(highlight(toFixedString('', 4), ['a']));

-- FixedString normal
SELECT '-- FixedString normal';
SELECT highlight(toFixedString('ab', 4), ['a']);

-- Multiple empty rows in sequence
SELECT '-- multiple empties';
SELECT highlight(s, ['x']) FROM (SELECT arrayJoin(['', '', '']) AS s);

-- Non-matching needle on short string: no false positive
SELECT '-- short string no false match';
SELECT highlight('a', ['ab']);
