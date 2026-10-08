-- Tests that iterative "match all" regexp functions evaluate zero-width assertions (`\b`, `^`, `$`) using the
-- surrounding characters of the whole string, not relative to the position of the previous match.
-- Previously these functions matched against a suffix of the string starting after the previous match, so every
-- continuation point looked like the beginning of the text and `\b` / `^` matched everywhere.
-- See https://github.com/ClickHouse/ClickHouse/issues/45843

-- extractAll: first letter of each word
SELECT extractAll('new york is the greatest', '\\b(\\w)');
-- extractAll: `^` anchor must match only at the start of the string
SELECT extractAll('abc', '^.');
-- extractAll: `$` anchor must match only at the end of the string
SELECT extractAll('abc', '.$');
-- extractAll: last letter of each word (already worked, kept as a regression guard)
SELECT extractAll('new york is the greatest', '(\\w)\\b');
-- extractAll without a capturing group: whole match is the first letter of each word
SELECT extractAll('new york is the greatest', '\\b\\w');

-- extractAllGroupsVertical / Horizontal: first letter of each word
SELECT extractAllGroupsVertical('new york is the greatest', '(\\b\\w)');
SELECT extractAllGroupsHorizontal('new york is the greatest', '(\\b\\w)');
-- A more realistic grouping example with a word boundary at the start of the pattern
SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '\\b(\\w+)=(\\w+)');

-- countMatches: number of words (word boundary at the start of a word character)
SELECT countMatches('new york is the greatest', '\\b\\w');
-- countMatches: `^` matches once
SELECT countMatches('aaa', '^a');

-- splitByRegexp: `^` anchored separator splits only at the start, splitByRegexp(separator, haystack)
SELECT splitByRegexp('^a', 'aaa');
-- splitByRegexp: separator anchored at a word boundary matches only the first `x` (the others are mid-word)
SELECT splitByRegexp('\\bx', 'xxx');

-- regexpPosition: 2nd occurrence of `\b\w` should be the 'y' in 'york' at byte 5
SELECT regexpPosition('new york is the greatest', '\\b\\w', 1, 2);
-- regexpPosition: 3rd occurrence of `\b\w`
SELECT regexpPosition('new york is the greatest', '\\b\\w', 1, 3);
-- regexpPosition: `^.` has only one occurrence; asking for the 2nd should return 0
SELECT regexpPosition('abc', '^.', 1, 2);
-- regexpPosition: last letter of each word, 2nd occurrence ('k' in 'york' at byte 8)
SELECT regexpPosition('new york is the greatest', '(\\w)\\b', 1, 2);
