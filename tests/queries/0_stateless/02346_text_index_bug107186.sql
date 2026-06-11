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

SELECT 'splitByNonAlpha tokenizer (exact read still used, results unchanged)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('hello world foo');
SELECT '- existing token, with index', count() FROM tab WHERE hasToken(s, 'world');
SELECT '- existing token, no index', count() FROM tab WHERE hasToken(s, 'world') SETTINGS use_skip_indexes = 0;
SELECT '- missing substring, with index', count() FROM tab WHERE hasToken(s, 'wor');
SELECT '- missing substring, no index', count() FROM tab WHERE hasToken(s, 'wor') SETTINGS use_skip_indexes = 0;
DROP TABLE tab;
