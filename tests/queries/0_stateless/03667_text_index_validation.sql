DROP TABLE IF EXISTS text_index_validation;

SET allow_experimental_full_text_index = 1;

CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = ngrams(0))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = ngrams(10))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = array('a'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha('a'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = splitByString([]))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }


CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(2, 2, 8))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(3, 2, 2))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(3, 1000))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(5, 3))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(3, 10, 11))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(3, 10, 2))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE text_index_validation (s String, INDEX idx_s (s) TYPE text(tokenizer = sparseGrams(3, 10, 5, 6, 7))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

