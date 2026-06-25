-- Tests https://github.com/ClickHouse/ClickHouse/issues/106922
-- A negated text-search predicate over a Nullable indexed column must not return rows whose
-- indexed value is NULL: f(NULL, ...) is NULL, NOT NULL is NULL, so a NULL row is filtered out.
-- The direct-read-from-text-index optimization materializes a non-Nullable 0/1 from the posting
-- list (NULL row has no tokens -> 0). A plain CAST to Nullable adds an all-false null map, so a
-- NULL row reads as a genuine 0 and NOT wrongly keeps it. The fix wraps the virtual column with
-- if(isNull(haystack), NULL, vc), re-deriving the haystack's real null map, so direct read stays
-- enabled for Nullable columns. This affects every direct-read function, not only hasToken:
-- hasAnyTokens, hasAllTokens, hasPhrase, startsWith, endsWith and the LIKE rewrites all go through
-- the same path. Each query is checked against the same query with the optimization disabled,
-- which evaluates the real function and is the reference for correct NULL semantics.

SET enable_analyzer = 1;
-- Exercise both direct-read modes (Exact replacement and Hint and(vc, real_func)) and the LIKE
-- dictionary-scan rewrite, all of which must preserve NULL semantics for a Nullable haystack.
SET query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT 'Nullable(String)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- NOT hasToken: NULL rows (2, 4) must not appear; only row 3 matches';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasAllTokens: only row 3 matches';
SELECT id FROM tab WHERE NOT hasAllTokens(str, ['hello']) ORDER BY id;
SELECT id FROM tab WHERE NOT hasAllTokens(str, ['hello']) ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasAnyTokens: only row 3 matches';
SELECT id FROM tab WHERE NOT hasAnyTokens(str, ['hello']) ORDER BY id;
SELECT id FROM tab WHERE NOT hasAnyTokens(str, ['hello']) ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasPhrase (Hint mode): rows 3 and 5 match, NULL rows excluded';
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT endsWith (Hint mode): rows 3 and 5 match, NULL rows excluded';
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id;
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT startsWith (Hint mode): only row 3, NULL rows excluded';
SELECT id FROM tab WHERE NOT startsWith(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT startsWith(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT LIKE suffix pattern (Hint mode): rows 3 and 5 match, NULL rows excluded';
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id;
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT match: only row 3, NULL rows excluded';
SELECT id FROM tab WHERE NOT match(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT match(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- positive hasPhrase still correct: only row 1';
SELECT id FROM tab WHERE hasPhrase(str, 'hello world') ORDER BY id;

SELECT '-- positive hasToken still correct: rows 1 and 5';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- hasToken IS NULL selects exactly the NULL rows 2 and 4';
SELECT id FROM tab WHERE hasToken(str, 'hello') IS NULL ORDER BY id;

SELECT '-- projection of hasToken keeps NULL for NULL rows';
SELECT id, hasToken(str, 'hello') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'Nullable(String), all-NULL part';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, NULL), (2, NULL), (3, NULL);

SELECT '-- NOT hasToken over an all-NULL part returns no rows (NOT NULL filters every row out)';
SELECT count() FROM tab WHERE NOT hasToken(str, 'hello');
SELECT count() FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'LowCardinality(Nullable(String))';

CREATE TABLE tab
(
    id  UInt32,
    str LowCardinality(Nullable(String)),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- NOT hasToken with LowCardinality(Nullable): only row 3';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Nullable(String) with preprocessor = lower(str)';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello World'), (2, NULL), (3, 'foo bar'), (4, NULL);

SELECT '-- NOT hasToken with preprocessor + Nullable: only row 3';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Nullable(String) with ngrams tokenizer, partially materialized index';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree ORDER BY id;

-- First part is written before the index materializes, second after, so the index is
-- partially materialized: some granules read postings directly, others re-run the function.
INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar');
SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (4, NULL), (5, 'hello there');

SELECT '-- NOT hasPhrase (ngrams, partially materialized): rows 3 and 5, NULL rows excluded';
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT endsWith (ngrams, partially materialized): rows 3 and 5, NULL rows excluded';
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id;
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT LIKE suffix pattern (ngrams, partially materialized): rows 3 and 5, NULL rows excluded';
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id;
SELECT id FROM tab WHERE NOT (str LIKE '%world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Map(String, Nullable(String)) with mapValues index';

-- A `mapValues(m)` index uses direct read only as a Hint (the key must still be matched), and the
-- Hint-mode virtual column is built on the same direct-read path, so the null-map restoration must
-- cover it too. The bug only shows when the Hint is kept (not bypassed), so use enough rows with a
-- selective needle that the index materializes per-row posting membership. A NULL map value reads
-- as 0 there, and `NOT and(0, NULL)` wrongly keeps the row unless the virtual column is wrapped with
-- if(isNull(haystack), NULL, vc) to carry the map value's real null map.
CREATE TABLE tab
(
    id UInt32,
    m  Map(String, Nullable(String)),
    INDEX idx mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

INSERT INTO tab
SELECT number AS id,
       multiIf(number % 500 = 0, map('k', CAST(NULL AS Nullable(String))),
               number % 777 = 0, map('k', 'needle here'),
               map('k', 'common filler text')) AS m
FROM numbers(10000);

SELECT '-- NOT hasToken(m[key]): NULL map values must be excluded; count matches the non-direct-read path';
SELECT count() FROM tab WHERE NOT hasToken(m['k'], 'needle');
SELECT count() FROM tab WHERE NOT hasToken(m['k'], 'needle') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasPhrase(m[key]) (Hint mode): NULL map values must be excluded';
SELECT count() FROM tab WHERE NOT hasPhrase(m['k'], 'needle here');
SELECT count() FROM tab WHERE NOT hasPhrase(m['k'], 'needle here') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT LIKE(m[key]): NULL map values must be excluded';
SELECT count() FROM tab WHERE NOT (m['k'] LIKE '%needle%');
SELECT count() FROM tab WHERE NOT (m['k'] LIKE '%needle%') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- no NULL map value leaks into the negated result';
SELECT countIf(m['k'] IS NULL) FROM tab WHERE NOT hasToken(m['k'], 'needle');

SELECT '-- positive hasToken(m[key]) still uses the index and is correct';
SELECT count() FROM tab WHERE hasToken(m['k'], 'needle');

DROP TABLE tab;

SELECT 'PREWHERE';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- NOT hasToken in PREWHERE: only row 3';
SELECT id FROM tab PREWHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab PREWHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Direct read stays enabled for a Nullable haystack';

-- The fix must preserve NULL semantics WITHOUT disabling the optimization. Assert the text-index
-- virtual column is still created (and wrapped with isNull(haystack)) for a Nullable column, so a
-- regression that falls back to a full scan for every Nullable predicate is caught.
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar'), (4, NULL), (5, 'hello there');

SELECT '-- virtual column is read directly and guarded by isNull(haystack)';
SELECT countIf(explain LIKE '%__text_index_%') > 0 AS reads_virtual_column,
       countIf(explain LIKE '%isNull(str)%' OR explain LIKE '%str IS NULL%') > 0 AS guarded_by_isnull
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello') SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, query_plan_remove_unused_columns = 1);

DROP TABLE tab;
