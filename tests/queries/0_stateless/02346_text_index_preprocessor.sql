-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache and global udf factory

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

-- Tests the preprocessor argument for tokenizers in the text index definitions

DROP TABLE IF EXISTS tab;

SELECT 'Positive tests on preprocessor construction and use.';

SELECT '- Test single tokenizer and preprocessor argument.';

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(str, 'foo');
SELECT count() FROM tab WHERE hasToken(str, 'FOO');

SELECT count() FROM tab WHERE hasToken(str, 'BAR');
SELECT count() FROM tab WHERE hasToken(str, 'Baz');

SELECT count() FROM tab WHERE hasToken(str, 'bar');
SELECT count() FROM tab WHERE hasToken(str, 'baz');

DROP TABLE tab;

SELECT '- Test preprocessor declaration using column more than once.';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(str, str))
)
ENGINE = MergeTree
ORDER BY tuple();


INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(str, 'foo');
SELECT count() FROM tab WHERE hasToken(str, 'FOO');

SELECT count() FROM tab WHERE hasToken(str, 'BAR');
SELECT count() FROM tab WHERE hasToken(str, 'Baz');

SELECT count() FROM tab WHERE hasToken(str, 'bar');
SELECT count() FROM tab WHERE hasToken(str, 'baz');

DROP TABLE tab;

SELECT '- Test preprocessor declaration using udf.';
DROP FUNCTION IF EXISTS udf_preprocessor;
CREATE FUNCTION udf_preprocessor AS (s) -> concat(str, lower(str));

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = udf_preprocessor(str))
)
ENGINE = MergeTree
ORDER BY tuple();


INSERT INTO tab VALUES (1, 'foo'), (2, 'BAR'), (3, 'Baz');

SELECT count() FROM tab WHERE hasToken(str, 'foo');
SELECT count() FROM tab WHERE hasToken(str, 'FOO');

SELECT count() FROM tab WHERE hasToken(str, 'BAR');
SELECT count() FROM tab WHERE hasToken(str, 'Baz');

SELECT count() FROM tab WHERE hasToken(str, 'bar');
SELECT count() FROM tab WHERE hasToken(str, 'baz');

DROP TABLE tab;
DROP FUNCTION udf_preprocessor;

SELECT 'Negative tests on preprocessor construction validations.';

-- The preprocessor argument must reference the index column
CREATE TABLE tab
(
    key UInt64,
    str String,
    other_str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(other_str))
)
ENGINE = MergeTree ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

SELECT '- The preprocessor argument must not reference non-indexed columns';
CREATE TABLE tab
(
    key UInt64,
    str String,
    other_str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(str, other_str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

SELECT '- Index definition may not be and expression when there is preprocessor';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(upper(str)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

SELECT '-- Not even the same expression';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(lower(str)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(upper(str)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(upper(str)))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

SELECT '- The preprocessor must be an expression';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nonExistingFunction)
)
ENGINE = MergeTree ORDER BY key;   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor must be an expression, with existing functions';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nonExistingFunction(str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError UNKNOWN_FUNCTION }

SELECT '- The preprocessor must have input and output values of the same type (here: String)';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = length(str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must use the column identifier';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = hostname())
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression must use only deterministic functions';
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(str, toString(rand())))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

SELECT '- The preprocessor expression is not compatible with array columns (yet)';
CREATE TABLE tab
(
    key UInt64,
    arr_str Array(String),
    INDEX idx(arr_str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr_str))
)
ENGINE = MergeTree ORDER BY tuple();   -- { serverError INCORRECT_QUERY }



DROP TABLE IF EXISTS tab;
