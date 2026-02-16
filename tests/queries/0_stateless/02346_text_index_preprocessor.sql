-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache and global udf factory

-- Tests the preprocessor argument for tokenizers in the text index definitions

DROP TABLE IF EXISTS tab;

SELECT 'Positive tests on preprocessor construction and use.';

SELECT '- Test simple preprocessor expression.';

CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY key;

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
    key UInt64,
    val LowCardinality(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY key;

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
    key UInt64,
    val FixedString(3),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

-- hasToken doesn't support FixedString columns
SELECT count() FROM tab WHERE hasToken(val, 'foo'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM tab WHERE hasToken(val, 'FOO'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM tab WHERE hasToken(val, 'BAR'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM tab WHERE hasToken(val, 'Baz'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM tab WHERE hasToken(val, 'bar'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM tab WHERE hasToken(val, 'baz'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM tab WHERE hasToken(val, 'def'); -- { serverError ILLEGAL_COLUMN }

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test simple preprocessor expression with Array(String).';
CREATE TABLE tab
(
    key UInt64,
    val Array(String),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (1, ['foo']), (2, ['BAR']), (3, ['Baz']);

-- hasToken doesn't support Array(String) columns
SELECT count() FROM tab WHERE hasToken(val, 'foo'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'FOO'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'BAR'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'Baz'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'bar'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'baz'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'def'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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
    key UInt64,
    val Array(FixedString(3)),
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (1, ['foo']), (2, ['BAR']), (3, ['Baz']);

-- hasToken doesn't support Array(String) columns
SELECT count() FROM tab WHERE hasToken(val, 'foo'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'FOO'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'BAR'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'Baz'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'bar'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'baz'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM tab WHERE hasToken(val, 'def'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'Baz');

SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'BAR');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'Baz');

DROP TABLE tab;

SELECT '- Test preprocessor declaration using column more than once.';
CREATE TABLE tab
(
    key UInt64,
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

SELECT '- Test preprocessor declaration using udf.';
DROP FUNCTION IF EXISTS udf_preprocessor;
CREATE FUNCTION udf_preprocessor AS (s) -> concat(val, lower(val));

CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = udf_preprocessor(val))
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
DROP FUNCTION udf_preprocessor;

SELECT 'Negative tests on preprocessor construction validations.';

-- The preprocessor argument must reference the index column
CREATE TABLE tab
(
    key UInt64,
    val String,
    other_str String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(other_str))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError UNKNOWN_IDENTIFIER }

SELECT '- The preprocessor argument must not reference non-indexed columns';
CREATE TABLE tab
(
    key UInt64,
    val String,
    other_str String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, other_str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_IDENTIFIER }

SELECT '- Index definition may not be and expression when there is preprocessor';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(upper(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- Not even the same expression';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(lower(val)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor must be an expression';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = BAD)
)
ENGINE = MergeTree ORDER BY key;   -- { serverError UNKNOWN_IDENTIFIER }

SELECT '- The preprocessor must be an expression, with existing functions';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nonExistingFunction(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_FUNCTION }

SELECT '- The preprocessor must have input and output values of the same type (here: String)';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = length(val))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must use the column identifier';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = hostname())
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must use only deterministic functions';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, toString(rand())))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must be a function, not an identifier';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = val)
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must not contain arrayJoin';
CREATE TABLE tab
(
    key UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = arrayJoin(array(val)))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }


DROP TABLE IF EXISTS tab;
