-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache and global udf factory

-- Tests the preprocessor argument in the text indexes

DROP TABLE IF EXISTS tab;

SELECT 'Basic expressions.';

SELECT '- Test simple preprocessor expression with String.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test simple preprocessor expression with LowCardinality.';

CREATE TABLE tab
(
    id UInt64,
    val LowCardinality(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test simple preprocessor expression with FixedString.';

CREATE TABLE tab
(
    id UInt64,
    val FixedString(3),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

-- hasToken doesn't support FixedString columns
SELECT count() FROM tab WHERE hasToken(val, 'foo'); -- { serverError ILLEGAL_COLUMN }

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test preprocessor declaration using the same column more than once.';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, val))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test preprocessor declaration using a UDF.';
DROP FUNCTION IF EXISTS udf;
CREATE FUNCTION udf AS (s) -> concat(val, lower(val));

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = udf(val))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;
DROP FUNCTION udf;

SELECT 'Negative tests.';

SELECT '- The preprocessor expression must reference only the index column';
CREATE TABLE tab
(
    id UInt64,
    val String,
    other_str String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(other_str))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError UNKNOWN_IDENTIFIER }

CREATE TABLE tab
(
    id UInt64,
    val String,
    other_str String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, other_str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_IDENTIFIER }

SELECT '- The preprocessor expression must be a function, not an identifier';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = val) -- val is a column
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor must be an expression';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = BAD) -- BAD doesn't occur anywhere
)
ENGINE = MergeTree ORDER BY id;   -- { serverError UNKNOWN_IDENTIFIER }

SELECT '- The preprocessor expression must be a known function';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nonExistingFunction(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_FUNCTION }

SELECT '- The preprocessor must have input and output values of the same type';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = length(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must reference the underlying column';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = hostname())
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must use only deterministic functions';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, toString(rand())))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must not contain arrayJoin';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = arrayJoin(array(val)))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT 'Advanced expressions and types.';

SELECT '- The preprocessor expression must contain the index definition';
SELECT '-- Single arguments';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(upper(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(upper(val)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '-- Multiple arguments';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(lower(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(val), lower(val)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'FOO');
SELECT count() FROM tab WHERE hasToken(val, 'BAR');
SELECT count() FROM tab WHERE hasToken(val, 'Baz');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'baz');
SELECT count() FROM tab WHERE hasToken(val, 'def');

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Negative tests';
SELECT '-- Index definition may not be a different expression than used in the preprocessor';
CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(upper(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'Array support';
SELECT '- Test simple preprocessor expression with Array(String).';

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (1, ['foo']), (2, ['BAR']), (3, ['Baz']);

-- hasToken doesn't support Array(String) columns
SELECT count() FROM tab WHERE hasToken(val, 'foo'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test simple preprocessor expression with Array(FixedString).';
CREATE TABLE tab
(
    id UInt64,
    val Array(FixedString(3)),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (1, ['foo']), (2, ['BAR']), (3, ['Baz']);

-- hasToken doesn't support Array(FixedString) columns
SELECT count() FROM tab WHERE hasToken(val, 'foo'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT id FROM tab WHERE hasAllTokens(val, 'foo');
SELECT id FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT id FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT id FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT id FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT id FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT id FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT id FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT 'Maps support';
SELECT '- Index on mapKeys(val)';

CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapKeys(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(val)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, {'foo': 'foo'}), (2, {'BAR': 'BAR'}), (3, {'Baz': 'Baz'});

SELECT '-- Map itself should not be passed as argument';
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- SELECT on mapKeys(val)';
SELECT 'foo', id FROM tab WHERE hasAllTokens(mapKeys(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAllTokens(mapKeys(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAllTokens(mapKeys(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAllTokens(mapKeys(val), 'Baz');

SELECT 'foo', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'Baz');

SELECT '-- SELECT on mapValues(val) (fallback)';
SELECT 'foo', id FROM tab WHERE hasAllTokens(mapValues(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAllTokens(mapValues(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAllTokens(mapValues(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAllTokens(mapValues(val), 'Baz');

SELECT 'foo', id FROM tab WHERE hasAnyTokens(mapValues(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAnyTokens(mapValues(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAnyTokens(mapValues(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAnyTokens(mapValues(val), 'Baz');


DROP TABLE tab;

SELECT '- Index on mapValues(val)';
CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapValues(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapValues(val)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, {'foo': 'foo'}), (2, {'BAR': 'BAR'}), (3, {'Baz': 'Baz'});

SELECT '-- SELECT on mapValues(val)';
SELECT 'foo', id FROM tab WHERE hasAllTokens(mapValues(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAllTokens(mapValues(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAllTokens(mapValues(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAllTokens(mapValues(val), 'Baz');

SELECT 'foo', id FROM tab WHERE hasAnyTokens(mapValues(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAnyTokens(mapValues(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAnyTokens(mapValues(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAnyTokens(mapValues(val), 'Baz');

SELECT '-- SELECT on mapKeys(val) (fallback)';
SELECT 'foo', id FROM tab WHERE hasAllTokens(mapKeys(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAllTokens(mapKeys(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAllTokens(mapKeys(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAllTokens(mapKeys(val), 'Baz');

SELECT 'foo', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'foo');
SELECT 'FOO', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'FOO');
SELECT 'BAR', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'BAR');
SELECT 'Baz', id FROM tab WHERE hasAnyTokens(mapKeys(val), 'Baz');

DROP TABLE tab;

SELECT '- Negative tests';
SELECT '-- Index on whole map must fail';
CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(val)))
)
ENGINE = MergeTree ORDER BY id;   -- { serverError BAD_ARGUMENTS }

SELECT '-- The preprocessor expression must contain the index definition';
CREATE TABLE tab
(
    id UInt64,
    val Map(String, String),
    INDEX idx(mapKeys(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS tab;

SELECT '14. Partially materialized index + preprocessor: haystack IS preprocessed on row-scan.';

-- Old parts use row-level scan. Unlike the postprocessor, the preprocessor is applied to the
-- haystack too. When old-part data is uppercase and the preprocessor lowercases the column,
-- the row-scan compares hasToken(lower('FOO'), lower('FOO')) = hasToken('foo', 'foo') = 1.
-- New parts have the index: it stores lower('FOO')='foo', and the lookup key is also 'foo'.
-- Both parts match, so the count is 2 (not 1). This proves the haystack IS preprocessed.

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'FOO'), (2, 'BAR');  -- old parts: no index, uppercase data

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val));

INSERT INTO tab VALUES (3, 'FOO'), (4, 'BAR');  -- new parts: with index, same data

-- Old part row-scan: hasToken(lower('FOO'), lower('FOO')) = hasToken('foo', 'foo') = 1.
-- New part index: lower('FOO')='foo' found → 1. Total: 2 (both parts agree).
SELECT count() FROM tab WHERE hasToken(val, 'FOO');  -- 2
SELECT count() FROM tab WHERE hasToken(val, 'BAR');  -- 2
SELECT count() FROM tab WHERE hasToken(val, 'xyz');  -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '15. Partially materialized index + non-trivial preprocessor: preprocessor applied consistently to both needle and haystack.';

-- The preprocessor strips the suffix "ing$" from tokens.
-- Old parts (row-level scan): hasToken(replaceRegexpAll('running','ing$',''), replaceRegexpAll('running','ing$',''))
--   = hasToken('runn', 'runn') = 1.
-- New parts: index stores 'runn' (preprocessed from 'running'); lookup key is also 'runn'.
-- Both parts match consistently → count is 2 for every query (contrast to postprocessor test 15 which gives 1).

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (1, 'running'), (2, 'cat');  -- old parts: no index

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = replaceRegexpAll(val, 'ing$', ''));

INSERT INTO tab VALUES (3, 'running'), (4, 'cat');  -- new parts: with index

-- 'running' → preprocessor → 'runn'. Both old and new parts match: total 2.
SELECT count() FROM tab WHERE hasToken(val, 'running');  -- 2
-- 'cat' is unchanged by the preprocessor. Both parts match: total 2.
SELECT count() FROM tab WHERE hasToken(val, 'cat');      -- 2
SELECT count() FROM tab WHERE hasToken(val, 'xyz');      -- 0

SYSTEM START MERGES tab;
DROP TABLE tab;

SELECT '16. Timestamp removal: preprocessor strips ISO timestamp prefix from log lines.';

-- Log lines contain a leading ISO timestamp followed by a space and the log message.
-- The preprocessor uses replaceRegexpAll to remove the timestamp prefix before tokenization,
-- so timestamp components are never stored in the index.

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = replaceRegexpAll(val, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')
    )
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, '2024-01-15T10:23:45 ERROR connection failed'),
    (2, '2024-01-15T10:23:46 INFO server started'),
    (3, '2024-01-15T10:23:47 ERROR disk full');

-- Searching by message content finds rows; the preprocessor is applied to the needle too,
-- but 'ERROR', 'connection', etc. are unaffected by the timestamp regex.
SELECT count() FROM tab WHERE hasToken(val, 'ERROR');        -- 2
SELECT count() FROM tab WHERE hasToken(val, 'connection');   -- 1
SELECT count() FROM tab WHERE hasToken(val, 'server');       -- 1
-- Timestamp components are not indexed; searching for them returns 0.
SELECT count() FROM tab WHERE hasToken(val, '2024');         -- 0
SELECT count() FROM tab WHERE hasToken(val, '10');           -- 0

DROP TABLE tab;

SELECT '17. Array tokenizer + preprocessor: has/hasAll/hasAny apply preprocessor on lookup.';
-- Index build always applies the preprocessor unconditionally. has/hasAll/hasAny must apply
-- it on the lookup side too so the needle matches the stored (preprocessed) form.

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'array', preprocessor = lower(val))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo', 'qux']), (2, ['BAR', 'baz']);

-- Index stores preprocessed values: 'foo'/'qux' for row 1, 'bar'/'baz' for row 2.
-- Lookup applies preprocessor to needle so 'Foo' -> 'foo', matching the stored form.
SELECT count() FROM tab WHERE has(val, 'Foo');         -- 1 (preprocessor aligns needle with index; row-level finds literal 'Foo')
SELECT count() FROM tab WHERE has(val, 'BAR');         -- 1
SELECT count() FROM tab WHERE has(val, 'xyz');         -- 0
-- Wrong capitalization: granule is kept (preprocessed 'foo' matches stored 'foo'), but
-- row-level has() is a literal comparison so 'foo' does not match the stored element 'Foo'.
SELECT count() FROM tab WHERE has(val, 'foo');         -- 0
SELECT count() FROM tab WHERE has(val, 'bar');         -- 0

DROP TABLE tab;

SELECT '18. Array tokenizer + preprocessor: has() / hasAll() / hasAny() apply the preprocessor.';
-- Index build applies the preprocessor unconditionally (stores 'foo', 'bar', 'baz').
-- has/hasAll/hasAny apply the preprocessor to the needle for the granule lookup, so
-- 'Foo' -> 'foo' finds the right granule; row-level still does literal comparison.

CREATE TABLE tab
(
    id UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'array', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo']), (2, ['BAR']), (3, ['baz']);

SELECT count() FROM tab WHERE has(val, 'Foo');         -- 1 (preprocessed needle 'foo' finds granule; literal 'Foo' matches row)
SELECT count() FROM tab WHERE has(val, 'foo');         -- 0 (granule kept, but literal 'foo' ≠ 'Foo')
SELECT count() FROM tab WHERE hasAll(val, ['BAR']);    -- 1
SELECT count() FROM tab WHERE hasAll(val, ['bar']);    -- 0
SELECT count() FROM tab WHERE hasAny(val, ['baz']);    -- 1
SELECT count() FROM tab WHERE hasAny(val, ['BAZ']);    -- 0

DROP TABLE tab;
