SET allow_experimental_full_text_index = 1;
DROP TABLE IF EXISTS tab;

SELECT 'Must not have no arguments.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text() -- { serverError INCORRECT_QUERY }
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT 'Test single tokenizer argument.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngram')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'noop')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- tokenizer must be default, ngram or noop.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'invalid')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'Test ngram_size argument.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngram', ngram_size = 4)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- ngram size must be between 2 and 8.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngram', ngram_size = 1)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngram', ngram_size = 9)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'Test max_rows_per_postings_list argument.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngram', ngram_size = 4, max_rows_per_postings_list = 9999)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- max_rows_per_posting_list is set to unlimited rows.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', max_rows_per_postings_list = 0)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- max_rows_per_posting_list should be at least 8192.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', max_rows_per_postings_list = 8191)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', max_rows_per_postings_list = 8192)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT 'Parameters are shuffled.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(max_rows_per_postings_list = 8192, tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(max_rows_per_postings_list = 8192, ngram_size = 4, tokenizer = 'ngram')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT 'Types are incorrect.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 1)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(ngram_size)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(ngram_size = '4')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(max_rows_per_postings_list)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(max_rows_per_postings_list = '9999')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'Same argument appears >1 times.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', tokenizer = 'ngram', ngram_size = 3)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngram', ngram_size = 3, ngram_size = 4)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', max_rows_per_postings_list = 9999, max_rows_per_postings_list = 8888)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'Must be created on single column.';

CREATE TABLE tab
(
    key UInt64,
    str1 String,
    str2 String,
    INDEX idx (str1, str2) TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree ORDER BY key; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

SELECT 'Must be created on String or FixedString or Array(String) or Array(FixedString) or LowCardinality(String) or LowCardinality(FixedString) columns.';

CREATE TABLE tab
(
    key UInt64,
    str UInt64,
    INDEX idx str TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY key; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    key UInt64,
    f32 Float32,
    INDEX idx f32 TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY key; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    key UInt64,
    n_str Nullable(String),
    INDEX idx n_str TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY key; -- { serverError INCORRECT_QUERY }
