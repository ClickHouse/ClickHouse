-- Tests for hasAnyTokens and hasAllTokens with the third argument (tokenizer definition).
-- Covers all tokenizers: ngrams, splitByNonAlpha, splitByString, sparseGrams.

-- { echoOn }

SELECT '--- ngrams tokenizer ---';

-- ngrams(3) - default ngram size
SELECT hasAllTokens('abcdef', 'abc', 'ngrams(3)');
SELECT hasAllTokens('abcdef', ['abc', 'bcd', 'cde', 'def'], 'ngrams(3)');
SELECT hasAnyTokens('abcdef', 'abc', 'ngrams(3)');
SELECT hasAnyTokens('abcdef', ['abc', 'xyz'], 'ngrams(3)');

-- positive: all 3-grams present
SELECT hasAllTokens('abcdef', ['abc', 'def'], 'ngrams(3)');
-- negative: not all 3-grams present
SELECT hasAllTokens('abcdef', ['abc', 'xyz'], 'ngrams(3)');
-- positive: at least one 3-gram present
SELECT hasAnyTokens('abcdef', ['xyz', 'cde'], 'ngrams(3)');
-- negative: no 3-gram present
SELECT hasAnyTokens('abcdef', ['xyz', 'zzz'], 'ngrams(3)');

-- ngrams with different sizes
SELECT hasAllTokens('abcdef', 'ab', 'ngrams(2)');
SELECT hasAllTokens('abcdef', ['ab', 'cd', 'ef'], 'ngrams(2)');
SELECT hasAnyTokens('abcdef', ['xy', 'ab'], 'ngrams(2)');
SELECT hasAnyTokens('abcdef', ['xy', 'yz'], 'ngrams(2)');

SELECT hasAllTokens('abcdefgh', 'abcd', 'ngrams(4)');
SELECT hasAllTokens('abcdefgh', ['abcd', 'efgh'], 'ngrams(4)');
SELECT hasAnyTokens('abcdefgh', ['xxxx', 'defg'], 'ngrams(4)');
SELECT hasAnyTokens('abcdefgh', ['xxxx', 'yyyy'], 'ngrams(4)');

-- ngrams without explicit size (default = 3)
SELECT hasAllTokens('abcdef', 'abc', 'ngrams');
SELECT hasAnyTokens('abcdef', 'xyz', 'ngrams');

-- with materialize
SELECT hasAllTokens(materialize('abcdef'), 'abc', 'ngrams(3)');
SELECT hasAllTokens(materialize('abcdef'), ['abc', 'xyz'], 'ngrams(3)');
SELECT hasAnyTokens(materialize('abcdef'), 'abc', 'ngrams(3)');
SELECT hasAnyTokens(materialize('abcdef'), ['xyz', 'zzz'], 'ngrams(3)');
SELECT hasAllTokens(materialize('abcdef'), ['abc', 'bcd', 'cde', 'def'], 'ngrams(3)');
SELECT hasAnyTokens(materialize('abcdef'), ['xyz', 'cde'], 'ngrams(3)');

SELECT '--- splitByNonAlpha tokenizer ---';

-- splitByNonAlpha splits by non-alphanumeric characters
SELECT hasAllTokens('hello world', 'hello', 'splitByNonAlpha');
SELECT hasAllTokens('hello world', ['hello', 'world'], 'splitByNonAlpha');
SELECT hasAllTokens('hello world', 'hello world', 'splitByNonAlpha');
-- negative: token not present
SELECT hasAllTokens('hello world', ['hello', 'foo'], 'splitByNonAlpha');
SELECT hasAnyTokens('hello world', ['foo', 'world'], 'splitByNonAlpha');
SELECT hasAnyTokens('hello world', ['foo', 'bar'], 'splitByNonAlpha');

-- tokens separated by various non-alpha characters
SELECT hasAllTokens('key=value;data', ['key', 'value', 'data'], 'splitByNonAlpha');
SELECT hasAnyTokens('key=value;data', ['missing', 'data'], 'splitByNonAlpha');
SELECT hasAnyTokens('key=value;data', ['missing', 'absent'], 'splitByNonAlpha');

-- with materialize
SELECT hasAllTokens(materialize('hello world'), ['hello', 'world'], 'splitByNonAlpha');
SELECT hasAllTokens(materialize('hello world'), ['hello', 'missing'], 'splitByNonAlpha');
SELECT hasAnyTokens(materialize('hello world'), ['missing', 'world'], 'splitByNonAlpha');
SELECT hasAnyTokens(materialize('hello world'), ['foo', 'bar'], 'splitByNonAlpha');

-- using internal name
SELECT hasAllTokens('hello world', ['hello', 'world'], 'tokenbf_v1');

SELECT '--- splitByString tokenizer ---';

-- splitByString with default separator (space)
SELECT hasAllTokens('hello world', ['hello', 'world'], 'splitByString');
SELECT hasAllTokens('hello world', ['hello', 'missing'], 'splitByString');
SELECT hasAnyTokens('hello world', ['missing', 'world'], 'splitByString');
SELECT hasAnyTokens('hello world', ['foo', 'bar'], 'splitByString');

-- splitByString with custom separators
SELECT hasAllTokens('key=value|data', ['key', 'value', 'data'], 'splitByString([\'=\', \'|\'])');
SELECT hasAllTokens('key=value|data', ['key', 'missing'], 'splitByString([\'=\', \'|\'])');
SELECT hasAnyTokens('key=value|data', ['missing', 'data'], 'splitByString([\'=\', \'|\'])');
SELECT hasAnyTokens('key=value|data', ['foo', 'bar'], 'splitByString([\'=\', \'|\'])');

-- splitByString with multi-character separator
SELECT hasAllTokens('hello::world::test', ['hello', 'world', 'test'], 'splitByString([\'::\'])');
SELECT hasAnyTokens('hello::world::test', ['missing', 'world'], 'splitByString([\'::\'])');
SELECT hasAnyTokens('hello::world::test', ['missing', 'absent'], 'splitByString([\'::\'])');

-- with materialize
SELECT hasAllTokens(materialize('key=value|data'), ['key', 'value', 'data'], 'splitByString([\'=\', \'|\'])');
SELECT hasAllTokens(materialize('key=value|data'), ['key', 'missing'], 'splitByString([\'=\', \'|\'])');
SELECT hasAnyTokens(materialize('hello::world'), ['missing', 'world'], 'splitByString([\'::\'])');
SELECT hasAnyTokens(materialize('hello::world'), ['missing', 'absent'], 'splitByString([\'::\'])');

SELECT '--- sparseGrams tokenizer ---';

-- sparseGrams with default parameters (min_length=3, max_length=100)
SELECT hasAllTokens('abcdef', 'abcdef', 'sparseGrams');
SELECT hasAnyTokens('abcdef', 'abcdef', 'sparseGrams');
SELECT hasAnyTokens('abcdef', 'xyzxyz', 'sparseGrams');

-- sparseGrams with explicit parameters
SELECT hasAllTokens('abcdefghij', 'abcdefghij', 'sparseGrams(3, 10)');
SELECT hasAnyTokens('abcdefghij', 'xyzxyzxyzx', 'sparseGrams(3, 10)');

-- sparseGrams with all three parameters (min_length, max_length, min_cutoff_length)
SELECT hasAllTokens('abcdefghij', 'abcdefghij', 'sparseGrams(3, 10, 5)');
SELECT hasAnyTokens('abcdefghij', 'xyzxyzxyzx', 'sparseGrams(3, 10, 5)');

-- with materialize
SELECT hasAllTokens(materialize('abcdef'), 'abcdef', 'sparseGrams');
SELECT hasAnyTokens(materialize('abcdef'), 'abcdef', 'sparseGrams');
SELECT hasAnyTokens(materialize('abcdef'), 'xyzxyz', 'sparseGrams');

-- using internal bloom filter index name
SELECT hasAllTokens('abcdef', 'abcdef', 'sparse_grams');
SELECT hasAnyTokens('abcdef', 'xyzxyz', 'sparse_grams');

SELECT '--- mixed: needles as string vs array ---';

-- String needles are tokenized with the same tokenizer
SELECT hasAllTokens('hello world foo', 'hello world', 'splitByNonAlpha');
SELECT hasAnyTokens('hello world foo', 'bar baz world', 'splitByNonAlpha');
SELECT hasAnyTokens('hello world foo', 'bar baz qux', 'splitByNonAlpha');

-- Array needles are used directly as tokens
SELECT hasAllTokens('hello world foo', ['hello', 'world'], 'splitByNonAlpha');
SELECT hasAnyTokens('hello world foo', ['bar', 'world'], 'splitByNonAlpha');
SELECT hasAnyTokens('hello world foo', ['bar', 'baz'], 'splitByNonAlpha');

SELECT '--- edge cases ---';

-- empty string
SELECT hasAllTokens('', 'abc', 'ngrams(3)');
SELECT hasAnyTokens('', 'abc', 'ngrams(3)');
SELECT hasAllTokens('', ['abc'], 'ngrams(3)');
SELECT hasAnyTokens('', ['abc'], 'ngrams(3)');

-- empty needles
SELECT hasAllTokens('hello', '', 'splitByNonAlpha');
SELECT hasAnyTokens('hello', '', 'splitByNonAlpha');
SELECT hasAllTokens('hello', [], 'splitByNonAlpha');
SELECT hasAnyTokens('hello', [], 'splitByNonAlpha');

-- materialize on empty
SELECT hasAllTokens(materialize(''), 'abc', 'ngrams(3)');
SELECT hasAnyTokens(materialize(''), ['abc'], 'ngrams(3)');
SELECT hasAllTokens(materialize('hello'), '', 'splitByNonAlpha');
SELECT hasAnyTokens(materialize('hello'), [], 'splitByNonAlpha');

SELECT '--- duplicate needles ---';

SELECT hasAllTokens('foo bar baz', ['foo', 'foo', 'foo', 'bar', 'bar', 'bar']);
SELECT hasAnyTokens('foo bar baz', ['foo', 'foo', 'foo', 'bar', 'bar', 'bar']);
SELECT hasAllTokens(lower(materialize('Hello world')), [lower(toLowCardinality('Hello'))]);

-- { echoOff }

SELECT '--- negative: invalid tokenizer ---';
SELECT hasAllTokens('abc', 'abc', 'nonExistentTokenizer'); -- { serverError BAD_ARGUMENTS }
