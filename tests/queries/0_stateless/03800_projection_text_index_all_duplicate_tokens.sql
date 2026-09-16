SET allow_experimental_projection_text_index = 1;
-- Regression for `hasAllTokens(col, ['foo', 'foo'])` — must behave like
-- `hasAllTokens(col, ['foo'])` (semantically `A ∩ A == A`). Defensive
-- correctness test for projection text index's `AndCursor`.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_dup_proj;

CREATE TABLE tab_dup_proj
(
    k UInt64,
    s String,
    PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8192;

-- 'foo' on every even row (5 rows), 'bar' on every odd row (5 rows). 10 rows total.
INSERT INTO tab_dup_proj SELECT number, if(number % 2 = 0, 'foo extra', 'bar extra') FROM numbers(10);

SELECT 'control: hasAllTokens(s, [foo])';
SELECT count() FROM tab_dup_proj WHERE hasAllTokens(s, ['foo']);

SELECT 'duplicate token: hasAllTokens(s, [foo, foo])';
SELECT count() FROM tab_dup_proj WHERE hasAllTokens(s, ['foo', 'foo']);

SELECT 'triple duplicate: hasAllTokens(s, [foo, foo, foo])';
SELECT count() FROM tab_dup_proj WHERE hasAllTokens(s, ['foo', 'foo', 'foo']);

SELECT 'mixed duplicate: hasAllTokens(s, [foo, bar, foo])';
SELECT count() FROM tab_dup_proj WHERE hasAllTokens(s, ['foo', 'bar', 'foo']);

SELECT 'all-missing duplicate: hasAllTokens(s, [missing, missing])';
SELECT count() FROM tab_dup_proj WHERE hasAllTokens(s, ['missing', 'missing']);

SELECT 'partial-missing duplicate: hasAllTokens(s, [foo, missing, foo])';
SELECT count() FROM tab_dup_proj WHERE hasAllTokens(s, ['foo', 'missing', 'foo']);

DROP TABLE tab_dup_proj;
