-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/107186
-- hasToken has fixed splitByNonAlpha semantics. The exact direct read answered it purely from the
-- index posting lists, which is only correct when the index tokenizer produces the same tokens
-- (splitByNonAlpha without a preprocessor). With a different tokenizer (asciiCJK, ngrams, ...) the
-- exact read returned rows the real hasToken evaluation rejects.
-- The hasToken result with the index must match the result without it.

SET allow_experimental_full_text_index = 1;

SELECT 'asciiCJK tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = asciiCJK)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('我来自北京邮电大学');

-- The whole CJK run is a single splitByNonAlpha token, so the substring is not a token: must be 0.
SELECT '- hasToken substring of CJK run, with index', count() FROM tab WHERE hasToken(s, '北京邮电大学');
SELECT '- hasToken substring of CJK run, no index', count() FROM tab WHERE hasToken(s, '北京邮电大学') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'ngrams tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = ngrams(3))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('helloworld');

-- 'low' is an ngram (substring) of 'helloworld' but not a splitByNonAlpha token: must be 0.
SELECT '- hasToken inner substring, with index', count() FROM tab WHERE hasToken(s, 'low');
SELECT '- hasToken inner substring, no index', count() FROM tab WHERE hasToken(s, 'low') SETTINGS use_skip_indexes = 0;
-- The whole word is a real token: must be 1.
SELECT '- hasToken full word, with index', count() FROM tab WHERE hasToken(s, 'helloworld');
SELECT '- hasToken full word, no index', count() FROM tab WHERE hasToken(s, 'helloworld') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'splitByNonAlpha tokenizer (exact read still used, results unchanged)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('hello world foo');

SELECT '- hasToken existing token, with index', count() FROM tab WHERE hasToken(s, 'world');
SELECT '- hasToken existing token, no index', count() FROM tab WHERE hasToken(s, 'world') SETTINGS use_skip_indexes = 0;
SELECT '- hasToken missing substring, with index', count() FROM tab WHERE hasToken(s, 'wor');
SELECT '- hasToken missing substring, no index', count() FROM tab WHERE hasToken(s, 'wor') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
