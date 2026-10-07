-- Tags: no-parallel-replicas

-- Tests that text-search functions (hasAnyTokens/hasAllTokens/hasPhrase) apply the text index's
-- tokenizer/preprocessor/postprocessor when used outside of the WHERE/PREWHERE filter (SELECT list,
-- aggregate-function arguments), and that a WHERE result is the same regardless of `use_skip_indexes`.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

SELECT 'array tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, if(number < 47, ['make-payment-check'], ['common:a']) FROM numbers(1000);

SELECT '-- SELECT-list / aggregate-argument position (has, hasAnyTokens, hasAllTokens)';

SELECT
    countIf(has(tags, 'make-payment-check')),
    countIf(hasAnyTokens(tags, ['make-payment-check'])),
    countIf(hasAllTokens(tags, ['make-payment-check']))
FROM tab;

SELECT '-- WHERE is consistent across use_skip_indexes';

SELECT count() FROM tab WHERE hasAnyTokens(tags, ['make-payment-check']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAllTokens(tags, ['make-payment-check']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(tags, ['make-payment-check']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE hasAllTokens(tags, ['make-payment-check']) SETTINGS use_skip_indexes = 1;

SELECT '-- EXPLAIN shows the tokenizer injected into SELECT-list functions';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN actions = 1
    SELECT hasAnyTokens(tags, ['make-payment-check']), hasAllTokens(tags, ['make-payment-check']) FROM tab
) WHERE explain ILIKE '%hasAnyTokens%' OR explain ILIKE '%hasAllTokens%';

SELECT '-- EXPLAIN shows the tokenizer injected into the WHERE filter (use_skip_indexes = 0)';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM tab WHERE hasAnyTokens(tags, ['make-payment-check']) SETTINGS use_skip_indexes = 0
) WHERE explain ILIKE '%hasAnyTokens%';

-- liftUpFunctions can hoist the projection above the SortingStep; the tokenizer must still be injected there.
SELECT '-- ORDER BY: projection lifted above the sort still gets the tokenizer';

SELECT count() FROM (SELECT hasAnyTokens(tags, ['make-payment-check']) AS h FROM tab ORDER BY id) WHERE h;

-- The walk must also traverse the negative/fractional limit-offset variants (here NegativeLimitStep).
SELECT '-- ORDER BY ... LIMIT -10: tokenizer injected above the negative-limit step';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN actions = 1
    SELECT hasAnyTokens(tags, ['make-payment-check']) FROM tab ORDER BY id LIMIT -10
) WHERE explain ILIKE '%hasAnyTokens%';

SELECT '-- LIMIT -N BY: tokenizer injected above the negative-limit-by step';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN actions = 1
    SELECT hasAnyTokens(tags, ['make-payment-check']) FROM tab ORDER BY id LIMIT -5 BY id
) WHERE explain ILIKE '%hasAnyTokens%';

SELECT '-- window function / QUALIFY: projection above the window still gets the tokenizer';

SELECT count() FROM (SELECT hasAnyTokens(tags, ['make-payment-check']) AS h, row_number() OVER (ORDER BY id) AS rn FROM tab) WHERE h;
SELECT count() FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM tab QUALIFY hasAnyTokens(tags, ['make-payment-check']));

SELECT '-- direct read preserved: eligible WHERE reads the __text_index_* virtual column';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM tab WHERE hasAnyTokens(tags, ['make-payment-check']) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1
) WHERE explain ILIKE '%__text_index_idx_tags%';

DROP TABLE tab;

SELECT 'preprocessor is applied only on the index path';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, if(number < 10, 'Hello World', 'foo bar') FROM numbers(1000);

-- The tokenizer is always applied; the preprocessor (lower) only on the index path, so 'hello' matches via the index (use_skip_indexes = 1) but not row-level.
SELECT '-- SELECT-list position: preprocessor not applied';

SELECT countIf(hasAnyTokens(s, 'hello')) FROM tab;

-- The SELECT-list rewrite must not borrow the preprocessor from a sibling WHERE that makes the index useful.
SELECT '-- SELECT-list preprocessor is not applied even when a sibling WHERE uses the index';

SELECT countIf(hasAnyTokens(s, 'hello')) FROM tab WHERE hasAnyTokens(s, 'world') SETTINGS use_skip_indexes = 1;

SELECT '-- WHERE: preprocessor applied only with use_skip_indexes = 1';

SELECT count() FROM tab WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1;

SELECT '-- EXPLAIN: SELECT-list injects the tokenizer but not the preprocessor';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN actions = 1
    SELECT hasAnyTokens(s, 'hello') FROM tab
) WHERE explain ILIKE '%hasAnyTokens%';

DROP TABLE tab;

SELECT 'postprocessor is applied on the row-scan path too';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, if(number < 10, 'Hello', 'World') FROM numbers(1000);

-- Unlike the preprocessor, the postprocessor normalizes tokens on every path, so a case-mismatched needle matches the same in the SELECT list, at use_skip_indexes = 0, and via the index.
SELECT '-- SELECT-list position: postprocessor applied';

SELECT countIf(hasToken(s, 'HELLO')), countIf(hasAnyTokens(s, ['HELLO'])) FROM tab;

SELECT '-- WHERE is consistent across use_skip_indexes';

SELECT count() FROM tab WHERE hasToken(s, 'HELLO') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasToken(s, 'HELLO') SETTINGS use_skip_indexes = 1;

DROP TABLE tab;

SELECT 'postprocessor with an ALIAS column outside WHERE';

CREATE TABLE tab
(
    id UInt64,
    str Nullable(String),
    alias String ALIAS ifNull(str, 'default'),
    INDEX idx_alias alias TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(alias))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab (id, str) SELECT number, if(number < 10, 'Hello', NULL) FROM numbers(1000);

SELECT '-- SELECT-list position on an ALIAS column: postprocessor applied';

SELECT countIf(hasToken(alias, 'HELLO')), countIf(hasAnyTokens(alias, ['DEFAULT'])) FROM tab;

DROP TABLE tab;
