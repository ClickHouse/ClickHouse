-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_text_index;

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET optimize_rewrite_like_perfect_affix = 1;
SET optimize_or_like_chain = 0;

CREATE TABLE t_text_index
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = ngrams(3), preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index (s) VALUES ('Hello, world!');

SELECT count() FROM t_text_index WHERE s LIKE '%Hello%' OR s LIKE '%hello%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index WHERE s LIKE '%Hello%' OR s LIKE '%hello%' SETTINGS use_skip_indexes_on_data_read = 1
)
WHERE explain LIKE '%Filter column%';

DROP TABLE t_text_index;

CREATE TABLE t_text_index
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index (s) VALUES ('Hello, world!');

-- Direct read from index is not used because splitByNonAlpha
-- tokenizer produces no tokens for the following query.
SELECT count() FROM t_text_index WHERE s LIKE '%Hello%' OR s LIKE '%hello%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index WHERE s LIKE '%Hello%' OR s LIKE '%hello%' SETTINGS use_skip_indexes_on_data_read = 1
)
WHERE explain LIKE '%Filter column%';

SELECT count() FROM t_text_index WHERE s LIKE 'Hello,%' OR s LIKE 'hello,%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1, indexes = 1 SELECT count() FROM t_text_index WHERE s LIKE 'Hello,%' OR s LIKE 'hello,%' SETTINGS use_skip_indexes_on_data_read = 1
)
WHERE explain LIKE '%Filter column%';

DROP TABLE t_text_index;
