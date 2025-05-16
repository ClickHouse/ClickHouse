SET allow_experimental_full_text_index = 1;
DROP TABLE IF EXISTS tab;

SELECT 'GIN index key-value support.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'default' )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'ngram' )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'noop' )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

SELECT 'tokenizer should be default, ngram or noop.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'defaul' )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'ngram', ngram_size = 4 )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(  tokenizer = 'ngram', ngram_size = 4, max_rows_per_postings_list = 9999 )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

SELECT 'ngram size must be between 2 and 8.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'ngram', ngram_size = 1 )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'ngram', ngram_size = 9 )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'max_rows_per_posting_list should be at least 8192.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'default', max_rows_per_postings_list = 8192 )
)
ENGINE = MergeTree
ORDER BY id; 
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( tokenizer = 'default', max_rows_per_postings_list = 8191 )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'shuffled parameters.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin( max_rows_per_postings_list = 8192, tokenizer = 'default' )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(  max_rows_per_postings_list = 8192, ngram_size = 4, tokenizer = 'ngram' )
)
ENGINE = MergeTree
ORDER BY id;
DROP TABLE tab;

SELECT 'incorrect types.';
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(  tokenizer = 1 )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(  ngram_size = '4' )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE gin(  max_rows_per_postings_list = '9999' )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }
