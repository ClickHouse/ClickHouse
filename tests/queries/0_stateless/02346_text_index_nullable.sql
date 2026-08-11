-- Tests for text index on Nullable columns and arrays with Nullable elements.
-- NULL values must be silently skipped during index construction; they must
-- not match any token search and must not cause exceptions.

-- Tests text index in Nullable types

SELECT 'Basic Nullable(String) expressions.';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'hello world'),
    (2, NULL),
    (3, 'foo bar'),
    (4, NULL),
    (5, 'hello foo');

SELECT '-- hasToken: rows 1 and 5 have "hello"; NULL rows 2 and 4 must not appear';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- hasToken: rows 3 and 5 have "foo"';
SELECT id FROM tab WHERE hasToken(str, 'foo') ORDER BY id;

SELECT '-- hasAllTokens: only row 1 contains both "hello" and "world"';
SELECT id FROM tab WHERE hasAllTokens(str, 'hello world') ORDER BY id;

SELECT '-- hasAnyTokens: row 1 has "world", row 3 has "bar"';
SELECT id FROM tab WHERE hasAnyTokens(str, 'world bar') ORDER BY id;

SELECT '-- hasToken combined with IS NULL: intersection of token match and IS NULL is always empty';
SELECT count() FROM tab WHERE hasToken(str, 'hello') AND str IS NULL;

SELECT '-- hasToken on absent token returns no rows';
SELECT count() FROM tab WHERE hasToken(str, 'xyz');

-- NULL and 0 are both falsy, so only a negated filter tells them apart: the index must not turn NULL into 0.
SELECT '-- NOT hasToken: only row 3; the NULL rows are NULL, i.e. not a match';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;

SELECT '-- NOT hasAnyTokens: rows 3 and 5 do not contain "world"';
SELECT id FROM tab WHERE NOT hasAnyTokens(str, 'world') ORDER BY id;

-- These use the index as a hint: its result is combined with the predicate rather than replacing it, and
-- `and(0, NULL)` is 0. Each query is repeated with direct read off, which is the reference.
SELECT '-- NOT like with the index as a hint: only row 3';
SELECT id FROM tab WHERE NOT like(str, '%hello%') ORDER BY id;
SELECT id FROM tab WHERE NOT like(str, '%hello%') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT startsWith: only row 3';
SELECT id FROM tab WHERE NOT startsWith(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT startsWith(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT endsWith: rows 3 and 5';
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id;
SELECT id FROM tab WHERE NOT endsWith(str, 'world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasPhrase: rows 3 and 5';
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id;
SELECT id FROM tab WHERE NOT hasPhrase(str, 'hello world') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT match: only row 3';
SELECT id FROM tab WHERE NOT match(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT match(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- NOT hasToken in PREWHERE: only row 3';
SELECT id FROM tab PREWHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab PREWHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- isNull(hasToken): rows 2 and 4';
SELECT id FROM tab WHERE isNull(hasToken(str, 'hello')) ORDER BY id;

SELECT '-- the predicate evaluates to NULL for the NULL rows, not to 0';
SELECT id, hasToken(str, 'hello') AS matched FROM tab WHERE matched OR isNull(matched) ORDER BY id;

-- Only the null map is read instead of the column data. The plan depends on the setting, unlike the
-- results above, so it is pinned here.
SELECT '-- the index is still read directly, and only the null map of the column with it';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(str, 'hello')
    -- CI may inject direct read False; INPUT actions are only printed by the legacy plan format
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, explain_query_plan_default = 'legacy'
)
WHERE explain LIKE '%INPUT%\_\_text_index%';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(str, 'hello')
    -- CI may inject direct read False; INPUT actions are only printed by the legacy plan format
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, explain_query_plan_default = 'legacy'
)
WHERE explain LIKE '%INPUT%str.null%';

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;

SELECT 'Nullable(String) with a partially materialized index.';
-- The first part has no index, so its virtual column comes from the default expression, which must
-- neither fail on the NULL rows nor report them as 0.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar');
ALTER TABLE tab ADD INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha');
INSERT INTO tab VALUES (4, NULL), (5, 'hello there');

SELECT '-- partially materialized: rows 1 and 5 have "hello"';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- partially materialized: NOT hasToken is only row 3, the NULL rows are NULL';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;

SELECT '-- partially materialized: isNull(hasToken) is rows 2 and 4';
SELECT id FROM tab WHERE isNull(hasToken(str, 'hello')) ORDER BY id;

-- Hint mode over the same mix: the virtual column is 1 where the index is missing, from the postings
-- where it is not, and the NULLs must survive both.
SELECT '-- partially materialized, index as a hint: NOT like is only row 3';
SELECT id FROM tab WHERE NOT like(str, '%hello%') ORDER BY id;

DROP TABLE tab;

SELECT 'Nullable(String) consisting entirely of NULLs.';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, NULL), (2, NULL), (3, NULL);

SELECT '-- all-NULL part: hasToken must return 0';
SELECT count() FROM tab WHERE hasToken(str, 'hello');

SELECT '-- all-NULL part: hasAnyTokens must return 0';
SELECT count() FROM tab WHERE hasAnyTokens(str, 'hello world');

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;

SELECT 'Nullable(String) consisting multiple granules where one granule is entirely NULL.';
--   Granule 0 (rows  1-4): mixed NULL / non-NULL
--   Granule 1 (rows  5-8): all NULL  → no tokens indexed
--   Granule 2 (rows 9-12): mixed NULL / non-NULL
-- The all-NULL granule must be skipped for every token search.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab VALUES
    (1,  'hello world'), (2,  NULL),       (3,  'foo bar'),  (4,  NULL),
    (5,  NULL),          (6,  NULL),        (7,  NULL),       (8,  NULL),
    (9,  'baz qux'),    (10, 'hello baz'), (11, NULL),       (12, 'world');

SELECT '-- "hello" appears in granules 0 and 2; granule 1 (all NULL) is skipped';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- "baz" only in granule 2';
SELECT id FROM tab WHERE hasToken(str, 'baz') ORDER BY id;

SELECT '-- all-NULL granule (rows 5-8) must not match "hello"';
SELECT count() FROM tab WHERE hasToken(str, 'hello') AND id BETWEEN 5 AND 8;

SELECT '-- "world" appears in granules 0 and 2';
SELECT id FROM tab WHERE hasToken(str, 'world') ORDER BY id;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;

SELECT 'Nullable(FixedString)';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(FixedString(12)),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world '), (2, NULL), (3, 'foo bar     ');

SELECT '-- Nullable(FixedString): only row 1 has "hello"';
SELECT id FROM tab WHERE hasAllToken(str, 'hello') ORDER BY id;

SELECT '-- Nullable(FixedString): only row 3 has "foo"';
SELECT id FROM tab WHERE hasAnyToken(str, 'foo') ORDER BY id;

SELECT '-- NULL row must not match';
SELECT count() FROM tab WHERE hasAllToken(str, 'hello') AND str IS NULL;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;

SELECT 'Array(Nullable(String))';
--  NULL elements inside arrays are skipped during indexing.
-- Rows whose every element is NULL produce no tokens and must never match.

CREATE TABLE tab
(
    id  UInt32,
    arr Array(Nullable(String)),
    INDEX idx(arr) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, ['hello', 'world']),
    (2, [NULL, 'foo']),
    (3, [NULL, NULL]),
    (4, ['bar', NULL]),
    (5, []);

SELECT '-- Array(Nullable): row 1 has "hello"';
SELECT id FROM tab WHERE hasAnyToken(arr, 'hello') ORDER BY id;

SELECT '-- Array(Nullable): row 2 has "foo" (NULL element is skipped)';
SELECT id FROM tab WHERE hasAnyToken(arr, 'foo') ORDER BY id;

SELECT '-- Array(Nullable): row 4 has "bar"';
SELECT id FROM tab WHERE hasAnyToken(arr, 'bar') ORDER BY id;

SELECT '-- Row 3 (all NULLs) must not match any token';
SELECT count() FROM tab WHERE hasAnyToken(arr, 'hello') AND id = 3;

SELECT '-- Row 5 (empty array) must not match any token';
SELECT count() FROM tab WHERE hasAnyToken(arr, 'hello') AND id = 5;

SELECT '-- hasAnyTokens: rows 2 ("foo") and 4 ("bar") match, so count is 2';
SELECT count() FROM tab WHERE hasAnyTokens(arr, 'foo bar');

SELECT '-- hasAllTokens: only row 1 has both "hello" and "world"';
SELECT id FROM tab WHERE hasAllTokens(arr, 'hello world') ORDER BY id;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(arr, NULL);
SELECT count() FROM tab WHERE hasAllToken(arr, NULL);
SELECT count() FROM tab WHERE hasAnyToken(arr, NULL);

DROP TABLE tab;


SELECT 'LowCardinality(Nullable(String))';

CREATE TABLE tab
(
    id  UInt32,
    str LowCardinality(Nullable(String)),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, NULL), (3, 'foo bar');

SELECT '-- LowCardinality(Nullable): row 1 has "hello"';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- LowCardinality(Nullable): row 3 has "foo"';
SELECT id FROM tab WHERE hasToken(str, 'foo') ORDER BY id;

SELECT '-- LowCardinality(Nullable): NULL row 2 must not match';
SELECT count() FROM tab WHERE hasToken(str, 'hello') AND id = 2;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;


SELECT 'Nullable(String) with preprocessor = lower(str)';

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'hello world'),
    (2, NULL),
    (3, 'foo bar');

SELECT '-- Preprocessor + Nullable: row 1 has "hello"';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- Preprocessor + Nullable: row 3 has "foo"';
SELECT id FROM tab WHERE hasToken(str, 'foo') ORDER BY id;

SELECT '-- Preprocessor + Nullable: NULL row 2 is not indexed and must not appear';
SELECT count() FROM tab WHERE id = 2 AND hasToken(str, 'hello');

SELECT '-- Preprocessor + Nullable: absent token returns no rows';
SELECT count() FROM tab WHERE hasToken(str, 'xyz');

SELECT '-- Preprocessor + Nullable: NOT hasToken is only row 3, the NULL row is NULL';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;


SELECT 'Array(Nullable(String)) with preprocessor = lower(arr)';

CREATE TABLE tab
(
    id  UInt32,
    arr Array(Nullable(String)),
    INDEX idx(arr) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, ['hello', 'world']),
    (2, [NULL, 'foo']),
    (3, [NULL, NULL]);

SELECT '-- Array preprocessor + Nullable: row 1 has "hello"';
SELECT id FROM tab WHERE hasAllToken(arr, 'hello') ORDER BY id;

SELECT '-- Array preprocessor + Nullable: row 2 has "foo" (NULL element still skipped after lower)';
SELECT id FROM tab WHERE hasAllToken(arr, 'foo') ORDER BY id;

SELECT '-- Array preprocessor + Nullable: row 3 (all NULLs) must not match';
SELECT count() FROM tab WHERE hasAnyToken(arr, 'hello') AND id = 3;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(arr, NULL);
SELECT count() FROM tab WHERE hasAllToken(arr, NULL);
SELECT count() FROM tab WHERE hasAnyToken(arr, NULL);

DROP TABLE tab;

SELECT 'Array(Nullable(String)) with postprocessor = lower(arr)';
-- NULL elements are skipped during indexing and the postprocessor is applied per token.

CREATE TABLE tab
(
    id  UInt32,
    arr Array(Nullable(String)),
    INDEX idx(arr) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(arr))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, ['Hello', 'World']),
    (2, [NULL, 'FOO']),
    (3, [NULL, NULL]),
    (4, ['Bar', NULL]);

SELECT '-- Array postprocessor + Nullable: row 1 has "hello" (postprocessor matches case-insensitively)';
SELECT id FROM tab WHERE hasAnyTokens(arr, 'HELLO') ORDER BY id;

SELECT '-- Array postprocessor + Nullable: row 2 has "foo" (NULL element still skipped after lower)';
SELECT id FROM tab WHERE hasAnyTokens(arr, 'foo') ORDER BY id;

SELECT '-- Array postprocessor + Nullable: hasAnyTokens rows 2 ("foo") and 4 ("bar") match, so count is 2';
SELECT count() FROM tab WHERE hasAnyTokens(arr, 'FOO BAR');

SELECT '-- Array postprocessor + Nullable: hasAllTokens only row 1 has both "hello" and "world"';
SELECT id FROM tab WHERE hasAllTokens(arr, 'Hello World') ORDER BY id;

SELECT '-- Array postprocessor + Nullable: row 3 (all NULLs) must not match';
SELECT count() FROM tab WHERE hasAnyTokens(arr, 'hello') AND id = 3;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(arr, NULL);
SELECT count() FROM tab WHERE hasAllToken(arr, NULL);
SELECT count() FROM tab WHERE hasAnyToken(arr, NULL);

DROP TABLE tab;

SELECT 'Map(String, Nullable(String))';
CREATE TABLE tab
(
    id UInt32,
    m  Map(String, Nullable(String)),
    INDEX idx(mapValues(m)) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, {'key1': 'hello world', 'key2': 'foo'}),
    (2, {'key1': NULL, 'key2': 'bar'}),
    (3, {'key1': NULL, 'key2': NULL}),
    (4, {});

SELECT '-- Map values: row 1 has "hello"';
SELECT id FROM tab WHERE hasAllToken(mapValues(m), 'hello') ORDER BY id;

SELECT '-- Map values: row 2 has "bar" (NULL value is skipped)';
SELECT id FROM tab WHERE hasAllToken(mapValues(m), 'bar') ORDER BY id;

SELECT '-- Map values: row 3 (all NULL values) must not match';
SELECT count() FROM tab WHERE hasAllToken(mapValues(m), 'hello') AND id = 3;

SELECT '-- Map values: row 4 (empty map) must not match';
SELECT count() FROM tab WHERE hasAllToken(mapValues(m), 'hello') AND id = 4;

SELECT '-- hasAnyTokens on map values: rows 1 ("foo") and 2 ("bar") match';
SELECT count() FROM tab WHERE hasAnyTokens(mapValues(m), 'foo bar');

SELECT '-- hasAllTokens on map values: only row 1 has both "hello" and "world"';
SELECT id FROM tab WHERE hasAllTokens(mapValues(m), 'hello world') ORDER BY id;

-- `m['key']` is an expression, not a column, so its null map cannot come from a subcolumn.
SELECT '-- NOT hasToken on a map element: no NULL map value leaks in';
SELECT countIf(m['key1'] IS NULL) FROM tab WHERE NOT hasToken(m['key1'], 'xyz');
SELECT countIf(m['key1'] IS NULL) FROM tab WHERE NOT hasToken(m['key1'], 'xyz') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(mapValues(m), NULL);
SELECT count() FROM tab WHERE hasAllToken(mapValues(m), NULL);
SELECT count() FROM tab WHERE hasAnyToken(mapValues(m), NULL);

DROP TABLE tab;

SELECT 'LowCardinality(String)';
CREATE TABLE tab
(
    id  UInt32,
    str LowCardinality(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'hello world'),
    (2, 'foo bar'),
    (3, 'baz'),
    (4, 'hello foo');

SELECT '-- LowCardinality(String): rows 1 and 4 have "hello"';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- LowCardinality(String): hasAnyTokens "bar" or "baz": rows 2 and 3';
SELECT id FROM tab WHERE hasAnyTokens(str, 'bar baz') ORDER BY id;

SELECT '-- LowCardinality(String): hasAllTokens "hello" and "world": only row 1';
SELECT id FROM tab WHERE hasAllTokens(str, 'hello world') ORDER BY id;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;


SELECT 'LowCardinality(Nullable(String))';
CREATE TABLE tab
(
    id  UInt32,
    str LowCardinality(Nullable(String)),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'hello world'),
    (2, NULL),
    (3, 'foo bar'),
    (4, NULL),
    (5, 'hello foo');

SELECT '-- LowCardinality(Nullable): rows 1 and 5 have "hello"';
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- LowCardinality(Nullable): hasAnyTokens: row 1 ("world") and row 3 ("bar")';
SELECT id FROM tab WHERE hasAnyTokens(str, 'world bar') ORDER BY id;

SELECT '-- LowCardinality(Nullable): hasAllTokens: only row 1 has both "hello" and "world"';
SELECT id FROM tab WHERE hasAllTokens(str, 'hello world') ORDER BY id;

SELECT '-- LowCardinality(Nullable): NULL rows must not match any token';
SELECT count() FROM tab WHERE hasToken(str, 'hello') AND str IS NULL;

SELECT '-- LowCardinality(Nullable): NOT hasToken is only row 3, the NULL rows are NULL';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;

SELECT '-- LowCardinality(Nullable): isNull(hasToken) is rows 2 and 4';
SELECT id FROM tab WHERE isNull(hasToken(str, 'hello')) ORDER BY id;

SELECT '-- has[Any|All]Token on NULL should not match anything';
SELECT count() FROM tab WHERE hasToken(str, NULL);
SELECT count() FROM tab WHERE hasAllToken(str, NULL);
SELECT count() FROM tab WHERE hasAnyToken(str, NULL);

DROP TABLE tab;

SELECT 'Nullable(String) with a null-removing preprocessor';
-- These strip the source nullability, so the predicate over a source-NULL row is 0 and its null map must
-- not be restored. The two parts below (written before and after ADD INDEX) must agree.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');
ALTER TABLE tab ADD INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = ifNull(str, '')) GRANULARITY 1;
INSERT INTO tab VALUES (11, 'hello'), (12, NULL), (13, 'foo');

SELECT '-- ifNull: a source NULL is 0, so NOT keeps it; both parts agree';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

-- Merges were stopped to keep the two parts apart; mutations need them running again.
SYSTEM START MERGES tab;
ALTER TABLE tab MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT '-- ifNull: unchanged after full materialization';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;

SELECT '-- ifNull: direct read stays enabled and the null map is not restored';
SELECT countIf(explain LIKE '%INPUT%\_\_text_index%') > 0, countIf(explain LIKE '%isNull(str)%') FROM
(
    EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello')
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, explain_query_plan_default = 'legacy'
);

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = coalesce(str, '')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');

SELECT '-- coalesce: same as ifNull';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = assumeNotNull(str)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');

SELECT '-- assumeNotNull: same as ifNull';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(ifNull(str, ''), 'Nullable(String)')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, NULL), (3, 'foo');

-- Nullable again by the outer cast, which must be looked through to the inner ifNull.
SELECT '-- CAST(ifNull(str, ...)) is still null-removing';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(CAST(ifNull(str, ''), 'Nullable(String)'))) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello'), (2, NULL), (3, 'Foo');

-- `lower` only propagates the nullability its argument no longer has.
SELECT '-- lower(CAST(ifNull(str, ...))) is still null-removing';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'Nullable(String) with a null-producing preprocessor';
-- `nullIf` makes NULLs that exist only in the index, which the source null map cannot describe, so
-- direct read is not used at all and every part answers alike.

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nullIf(str, '')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo'), (4, NULL);

SELECT '-- nullIf: row 2 (preprocessed to NULL) and row 4 (source NULL) are both filtered out';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- nullIf: direct read is not used for a preprocessor-rewritten function';
SELECT countIf(explain LIKE '%INPUT%\_\_text_index%') FROM
(
    EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello')
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, explain_query_plan_default = 'legacy'
);

-- endsWith is not rewritten through the preprocessor, so it reads the source value and keeps direct read.
SELECT '-- nullIf: endsWith keeps direct read and still excludes the NULL rows';
SELECT id FROM tab WHERE NOT endsWith(str, 'oo') ORDER BY id;
SELECT id FROM tab WHERE NOT endsWith(str, 'oo') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(nullIf(str, ''), 'Nullable(String)')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo'), (4, NULL);

-- Looking through the widening cast must not misclassify the inner nullIf as null-removing.
SELECT '-- CAST(nullIf(str, ...)) is still null-producing';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    id  UInt32,
    str Nullable(String),
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(toNullable(str))) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello'), (2, NULL), (3, 'Foo');

-- Nullable, but only propagates the source nullability.
SELECT '-- lower(toNullable(str)) neither produces nor removes NULLs';
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, 'hello') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- lower(toNullable(str)): direct read is kept';
SELECT countIf(explain LIKE '%INPUT%\_\_text_index%') > 0 FROM
(
    EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, 'hello')
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, explain_query_plan_default = 'legacy'
);

DROP TABLE tab;

SELECT 'String with a null-producing preprocessor and a Nullable needle';
-- The source column is not Nullable, but the preprocessor still makes row 2 NULL and the Nullable needle
-- makes that NULL observable. Nothing in the source describes it, so direct read is not used here either.
-- (With a non-Nullable needle the rewritten predicate is cast back to UInt8 and the row-level path raises
-- CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, which is a separate pre-existing problem and left alone.)

CREATE TABLE tab
(
    id  UInt32,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nullIf(str, '')) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'hello'), (2, ''), (3, 'foo');

SELECT '-- row 2 is NULL for the rewritten predicate, so only row 3 is kept';
SELECT id FROM tab WHERE NOT hasToken(str, toNullable('hello')) ORDER BY id;
SELECT id FROM tab WHERE NOT hasToken(str, toNullable('hello')) ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- direct read is not used for it';
SELECT countIf(explain LIKE '%INPUT%\_\_text_index%') FROM
(
    EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(str, toNullable('hello'))
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, explain_query_plan_default = 'legacy'
);

DROP TABLE tab;
