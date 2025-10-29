SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

-- Basic test preprocessor clause usage with lower
DROP TABLE IF EXISTS tab;

SELECT 'Test single tokenizer and preprocessor argument.';

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

-- Test preprocessor declaration using column more than once
SELECT 'Test preprocessor declaration using column more than once.';
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

-- Negative tests

-- -- Basic test preprocessor validations
SELECT 'Test preprocessor validations.';

-- Dependency only on indexed column
CREATE TABLE tab
(
    key UInt64,
    str String,
    str2 String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str2))
)
ENGINE = MergeTree
ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

-- Dependency only on 1 column
CREATE TABLE tab
(
    key UInt64,
    str String,
    str2 String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(str), lower(str2)))
)
ENGINE = MergeTree
ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

-- When using preprocessor, the index expression should be only the column (with no expressions)
CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(upper(str)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(lower(str)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(upper(str)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(upper(str)))
)
ENGINE = MergeTree
ORDER BY tuple();   -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nonExistingFunction)
)
ENGINE = MergeTree
ORDER BY key;   -- { serverError INCORRECT_QUERY }


CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = nonExistingFunction(str))
)
ENGINE = MergeTree
ORDER BY tuple();   -- { serverError UNKNOWN_FUNCTION }

CREATE TABLE tab
(
    key UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = length(str))
)
ENGINE = MergeTree
ORDER BY tuple();   -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS tab;





