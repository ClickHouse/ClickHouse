-- Regression for lazy ALL mode: `hasAllTokens(col, ['foo', 'foo'])` must behave like
-- `hasAllTokens(col, ['foo'])` (semantically `A ∩ A == A`). The lazy-mode fast-path
-- used to compare a token-keyed map size against `tokens.size()` and return all zeros
-- whenever the query contained duplicate tokens, even when those tokens matched.
-- See PR https://github.com/ClickHouse/ClickHouse/pull/100035 thread r3221074323.

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;

DROP TABLE IF EXISTS tab_dup;

-- `posting_list_codec = 'bitpacking'` is required to keep lazy mode active. Without it
-- the codec defaults to None and `setIndexGranule` silently falls back to materialize,
-- which would mask the regression.
CREATE TABLE tab_dup(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192;

-- 'foo' on every even row (5 rows), 'bar' on every odd row (5 rows). 10 rows total.
INSERT INTO tab_dup SELECT number, if(number % 2 = 0, 'foo extra', 'bar extra') FROM numbers(10);

SELECT 'control: hasAllTokens(s, [foo])';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

SELECT 'duplicate token: hasAllTokens(s, [foo, foo])';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

SELECT 'triple duplicate: hasAllTokens(s, [foo, foo, foo])';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'foo', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'foo', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

SELECT 'mixed duplicate: hasAllTokens(s, [foo, bar, foo])';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'bar', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'bar', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

SELECT 'all-missing duplicate: hasAllTokens(s, [missing, missing])';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['missing', 'missing']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['missing', 'missing']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

SELECT 'partial-missing duplicate: hasAllTokens(s, [foo, missing, foo])';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'missing', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_dup WHERE hasAllTokens(s, ['foo', 'missing', 'foo']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

DROP TABLE tab_dup;
