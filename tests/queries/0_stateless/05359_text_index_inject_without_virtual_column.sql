-- Random settings limits: query_plan_direct_read_from_text_index=(1, 1); query_plan_text_index_add_hint=(1, 1); use_skip_indexes=(1, 1)
-- A text-search function that direct read from the text index cannot replace with a virtual column
-- must still be evaluated with the index tokenizer, preprocessor and postprocessor.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar'), (3, 'the the');

SELECT count() FROM tab WHERE NOT hasPhrase(val, 'the the');
SELECT count() FROM tab WHERE NOT hasPhrase(val, 'the');
SELECT count() FROM tab WHERE NOT hasPhrase(val, ['the', 'the']);
SELECT count() FROM tab PREWHERE NOT hasPhrase(val, 'the the');
SELECT count() FROM tab WHERE hasPhrase(val, 'the the');

DROP TABLE IF EXISTS tab_nullable;
CREATE TABLE tab_nullable (id UInt64, val Nullable(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val = 'the', '', val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_nullable VALUES (1, 'hello world'), (2, 'foo bar'), (3, 'the the');

SELECT count() FROM tab_nullable WHERE NOT hasPhrase(val, 'the the');

DROP TABLE IF EXISTS tab_lower;
CREATE TABLE tab_lower (id UInt64, s String, INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(s))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_lower VALUES (1, 'xyz'), (2, 'Hello world'), (3, 'foo');

SELECT count() FROM tab_lower WHERE hasToken(s, 'world') AND toString(hasToken(s, 'hello')) = '1';
SELECT count() FROM tab_lower WHERE hasToken(s, 'world') AND toString(hasAnyTokens(s, 'hello')) = '1';
SELECT count() FROM tab_lower WHERE hasToken(s, 'world') AND toString(hasAllTokens(s, 'hello')) = '1';
SELECT count() FROM tab_lower WHERE hasToken(s, 'world') AND toString(hasPhrase(s, 'hello world')) = '1';
SELECT count() FROM tab_lower PREWHERE hasToken(s, 'world') AND toString(hasAnyTokens(s, 'hello')) = '1';

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tab_lower WHERE hasToken(s, 'world') AND toString(hasAnyTokens(s, 'hello')) = '1')
WHERE explain LIKE '%__text_index_idx_hasToken%';

DROP TABLE IF EXISTS tab_pre;
CREATE TABLE tab_pre (id UInt64, s String, INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(s))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_pre VALUES (1, 'xyz'), (2, 'Hello world'), (3, 'foo');

SELECT count() FROM tab_pre WHERE hasToken(s, 'world') AND toString(hasToken(s, 'HELLO')) = '1';
SELECT count() FROM tab_pre WHERE hasToken(s, 'world') AND toString(hasAnyTokens(s, 'HELLO')) = '1';

DROP TABLE tab;
DROP TABLE tab_nullable;
DROP TABLE tab_lower;
DROP TABLE tab_pre;
