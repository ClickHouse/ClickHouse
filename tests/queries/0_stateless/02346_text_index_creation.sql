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
    INDEX idx str TYPE text(tokenizer = 'split')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'no_op')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- tokenizer must be default, ngram, split or no_op.';

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

SELECT 'Test separators argument.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'split', separators = ['\n', '\\'])
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- separators must be array.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'split', separators = '\n')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT '-- separators must be an array of strings.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'split', separators = [1, 2])
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'Test segment_digestion_threshold_bytes argument.';

SELECT '-- segment_digestion_threshold_bytes must be an integer.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', segment_digestion_threshold_bytes = '1024')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', segment_digestion_threshold_bytes = 1024)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- segment_digestion_threshold_bytes can be 0 (zero).';
CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', segment_digestion_threshold_bytes = 0)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT 'Test bloom_filter_false_positive_rate argument.';

SELECT '-- bloom_filter_false_positive_rate must be a double.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', bloom_filter_false_positive_rate = 1)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', bloom_filter_false_positive_rate = '1024')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', bloom_filter_false_positive_rate = 0.5)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- bloom_filter_false_positive_rate must be between 0.0 and 1.0.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', bloom_filter_false_positive_rate = 0.0)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', bloom_filter_false_positive_rate = 1.0)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SELECT 'Parameters are shuffled.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(ngram_size = 4, tokenizer = 'ngram')
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
    INDEX idx str TYPE text(tokenizer = 'default', bloom_filter_false_positive_rate = 0.5, bloom_filter_false_positive_rate = 0.5)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'default', segment_digestion_threshold_bytes = 1024, segment_digestion_threshold_bytes = 2048)
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

SELECT 'A column must not have >1 text index';

SELECT '-- CREATE TABLE';

CREATE TABLE tab(
    s String,
    INDEX idx_1(s) TYPE text(tokenizer = 'default'),
    INDEX idx_2(s) TYPE text(tokenizer = 'ngram', ngram_size = 3)
)
Engine = MergeTree()
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT '-- ALTER TABLE';

CREATE TABLE tab
(
    str String,
    INDEX idx_1 (str) TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE tab ADD INDEX idx_2(str) TYPE text(tokenizer = 'ngram', ngram_size = 3); -- { serverError BAD_ARGUMENTS }

-- It must still be possible to create a column on the same column with a different expression
ALTER TABLE tab ADD INDEX idx_3(lower(str)) TYPE text(tokenizer = 'ngram', ngram_size = 3);

DROP TABLE tab;

SELECT 'Must be created on String or FixedString or LowCardinality(String) or LowCardinality(FixedString) columns.';

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
    str Array(String),
    INDEX idx str TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY key; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    key UInt64,
    str Array(FixedString(2)),
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
