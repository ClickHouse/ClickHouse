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
SELECT id FROM tab WHERE hasToken(str, 'hello') ORDER BY id;

SELECT '-- Nullable(FixedString): only row 3 has "foo"';
SELECT id FROM tab WHERE hasToken(str, 'foo') ORDER BY id;

SELECT '-- NULL row must not match';
SELECT count() FROM tab WHERE hasToken(str, 'hello') AND str IS NULL;

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

DROP TABLE tab;

SELECT 'Some negative tests';


