-- A hasToken needle containing a token separator (non-alphanumeric character) is invalid and raises
-- BAD_ARGUMENTS when doing a brute-force scan (i.e. no text index usage). This test checks that it behaves
-- the same under index lookups. hasTokenOrNull returns NULL per row instead of throwing.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/102497

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar baz'), (3, 'qux');

SELECT '-- hasToken with separator needle, brute-force scan';
SELECT count() FROM tab WHERE hasToken(text, 'hello world') SETTINGS ignore_data_skipping_indices = 'idx'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE hasToken(text, 'foo.bar')     SETTINGS ignore_data_skipping_indices = 'idx'; -- { serverError BAD_ARGUMENTS }

SELECT '-- hasToken with separator needle, index lookup';
SELECT count() FROM tab WHERE hasToken(text, 'hello world'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE hasToken(text, 'foo.bar');     -- { serverError BAD_ARGUMENTS }

SELECT '-- Valid single-token needles still use the index, brute-force scan';
SELECT count() FROM tab WHERE hasToken(text, 'foo') SETTINGS ignore_data_skipping_indices = 'idx'; -- 1
SELECT count() FROM tab WHERE hasToken(text, 'qux') SETTINGS ignore_data_skipping_indices = 'idx'; -- 1

SELECT '-- Valid single-token needles still use the index, index lookup';
SELECT count() FROM tab WHERE hasToken(text, 'foo'); -- 1
SELECT count() FROM tab WHERE hasToken(text, 'qux'); -- 1

SELECT '-- hasTokenOrNull with separator needle returns NULL per row';
SELECT count() FROM tab WHERE isNull(hasTokenOrNull(text, 'hello world')) SETTINGS ignore_data_skipping_indices = 'idx'; -- 3
SELECT count() FROM tab WHERE isNull(hasTokenOrNull(text, 'hello world')); -- 3

DROP TABLE tab;
