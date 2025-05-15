SET allow_experimental_full_text_index = 1;
DROP TABLE IF EXISTS tab;

SELECT 'GIN index only literals. (backward compatibility)';

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(5, 8192) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

SELECT 'max_rows_per_posting_list should be at least 8192.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(0, 3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(2, 3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'ngram size must be between 2 and 8.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'GIN index key-value support.';

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='default') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='ngram') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='noop') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

SELECT 'tokenizer should be default, ngram or noop.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='defaul') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='ngram', ngram_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

SELECT 'ngram size must be between 2 and 8.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='ngram', ngram_size=0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='ngram', ngram_size=10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'max_rows_per_posting_list should be at least 8192.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='default', max_rows_per_postings_list = 8192) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; 
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(tokenizer='default', max_rows_per_postings_list = 1000) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }
