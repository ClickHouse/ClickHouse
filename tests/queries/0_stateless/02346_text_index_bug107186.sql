-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/107186
-- hasToken has fixed splitByNonAlpha semantics. Exact direct read from a mismatched index tokenizer
-- returned rows the real hasToken rejects (false positive), and a needle that the index tokenizer cannot
-- represent pruned every granule (false negative). The count with the index must match the count without it.

SET allow_experimental_full_text_index = 1;

SELECT 'asciiCJK tokenizer (was a false positive)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = asciiCJK)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('我来自北京邮电大学');
-- The whole CJK run is one splitByNonAlpha token, so the substring is not a token: must be 0.
SELECT '- substring of CJK run, with index', count() FROM tab WHERE hasToken(s, '北京邮电大学');
SELECT '- substring of CJK run, no index', count() FROM tab WHERE hasToken(s, '北京邮电大学') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'ngrams tokenizer, needle shorter than n (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = ngrams(4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('abc');
-- 'abc' is a whole splitByNonAlpha token but tokenizes to nothing under ngrams(4): must be 1.
SELECT '- short needle, with index', count() FROM tab WHERE hasToken(s, 'abc');
SELECT '- short needle, no index', count() FROM tab WHERE hasToken(s, 'abc') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'ngrams tokenizer, substring vs whole token';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = ngrams(3))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('helloworld');
-- 'low' is an ngram (substring) of 'helloworld' but not a splitByNonAlpha token: must be 0.
SELECT '- inner substring, with index', count() FROM tab WHERE hasToken(s, 'low');
SELECT '- inner substring, no index', count() FROM tab WHERE hasToken(s, 'low') SETTINGS use_skip_indexes = 0;
-- The whole word is a real token: must be 1.
SELECT '- whole word, with index', count() FROM tab WHERE hasToken(s, 'helloworld');
SELECT '- whole word, no index', count() FROM tab WHERE hasToken(s, 'helloworld') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'array tokenizer, needle is a real token of a multi-word value (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('hello world');
-- The array tokenizer stores the whole value as one token, but 'hello' is a splitByNonAlpha token of the row: must be 1.
SELECT '- token of multi-word value, with index', count() FROM tab WHERE hasToken(s, 'hello');
SELECT '- token of multi-word value, no index', count() FROM tab WHERE hasToken(s, 'hello') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'splitByString tokenizer, needle inside a token spanning a non-separator boundary (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByString([', ']))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('hello world, foo');
-- splitByString(', ') yields ['hello world', 'foo'], but 'hello' is a splitByNonAlpha token of the row: must be 1.
SELECT '- token inside a separator span, with index', count() FROM tab WHERE hasToken(s, 'hello');
SELECT '- token inside a separator span, no index', count() FROM tab WHERE hasToken(s, 'hello') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'asciiCJK tokenizer, connector merges what splitByNonAlpha splits (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = asciiCJK)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('a.b');
-- asciiCJK keeps 'a.b' as one token (the '.' connects letters), but splitByNonAlpha splits it, so 'a' is a real token: must be 1.
SELECT '- connector-joined token, with index', count() FROM tab WHERE hasToken(s, 'a');
SELECT '- connector-joined token, no index', count() FROM tab WHERE hasToken(s, 'a') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'splitByNonAlpha tokenizer (exact read still used, results unchanged)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('hello world foo');
SELECT '- existing token, with index', count() FROM tab WHERE hasToken(s, 'world');
SELECT '- existing token, no index', count() FROM tab WHERE hasToken(s, 'world') SETTINGS use_skip_indexes = 0;
SELECT '- missing substring, with index', count() FROM tab WHERE hasToken(s, 'wor');
SELECT '- missing substring, no index', count() FROM tab WHERE hasToken(s, 'wor') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'splitByNonAlpha tokenizer with preprocessor (documented divergence still works, index kept)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('Hello World');
-- The needle is preprocessed the same way (lowered) as the indexed text, so 'hello' matches with and without the index: must be 1.
SELECT '- preprocessed token, with index', count() FROM tab WHERE hasToken(s, 'Hello');
SELECT '- preprocessed token, no index', count() FROM tab WHERE hasToken(s, 'Hello') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'coarse tokenizer with preprocessor: documented divergence kept, granule pruning suppressed (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByString([', ']), preprocessor = lower(s))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('Hello World, Foo');
-- splitByString(', ') over lower(s) stores ['hello world', 'foo'], so 'hello' is not an index token, but it is a
-- splitByNonAlpha token of the preprocessed text. The index must not prune the granule (must be 1), while the
-- documented preprocessor divergence is preserved (the no-index query is case-sensitive, so it returns 0).
SELECT '- coarse + preprocessor, with index', count() FROM tab WHERE hasToken(s, 'hello');
SELECT '- coarse + preprocessor, no index', count() FROM tab WHERE hasToken(s, 'hello') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'preprocessed needle maps to no index token but a preprocessor is active (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = ngrams(4), preprocessor = lower(s))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('ABC');
-- The needle 'abc' is lowered then tokenized by ngrams(4), which yields no 4-gram (too short), so the index cannot
-- prove a match. With a preprocessor the documented rewrite must still run: hasToken(s, 'abc') -> hasToken(lower(s),
-- 'abc') matches the lowered row (must be 1). Without the index the predicate is the raw case-sensitive
-- hasToken('ABC', 'abc') (must be 0): the documented preprocessor divergence is preserved.
SELECT '- preprocessor + empty index tokens, with index', count() FROM tab WHERE hasToken(s, 'abc');
SELECT '- preprocessor + empty index tokens, no index', count() FROM tab WHERE hasToken(s, 'abc') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;

SELECT 'coarse + preprocessor, non-prunable hasToken ANDed with a matching atom (was a false negative)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByString([', ']), preprocessor = lower(s))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('aaa bbb, zzz');
-- splitByString(', ') over lower(s) stores ['aaa bbb', 'zzz']. The hasToken('aaa') atom is kept only to drive the
-- preprocessor rewrite (non-prunable), but 'aaa' is not an index token, so it must not fail the whole AND in the
-- text index analyzer: the ANDed hasAllTokens('zzz') matches, so the granule must not be dropped (must be 1).
SELECT '- non-prunable hasToken AND matching atom, with index', count() FROM tab WHERE hasToken(s, 'aaa') AND hasAllTokens(s, 'zzz');
SELECT '- non-prunable hasToken AND matching atom, no index', count() FROM tab WHERE hasToken(s, 'aaa') AND hasAllTokens(s, 'zzz') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;
