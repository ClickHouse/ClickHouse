-- Tags: no-fasttest
-- `hasTokenOrNull` on a `splitByNonAlpha` text index with a separator-only needle bypasses the index
-- (row-level scan returns NULL per row); a valid needle still uses the index.

DROP TABLE IF EXISTS tab;
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

SELECT '-- separator-only needle: WHERE returns 0 rows';
SELECT count() FROM tab WHERE hasTokenOrNull(text, '()');

SELECT '-- separator-only needle: text index NOT used (bypassed)';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenOrNull(text, '()')
) WHERE explain LIKE '%Name:%';

SELECT '-- valid needle: text index IS used';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasTokenOrNull(text, 'foo')
) WHERE explain LIKE '%Name:%';

DROP TABLE tab;
