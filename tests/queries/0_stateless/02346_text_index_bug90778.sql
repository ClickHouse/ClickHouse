-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    col LowCardinality(String),
    INDEX idx col type text(tokenizer='array')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES ('config');

SELECT count() FROM tab WHERE col = 'config';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE col = 'config'
    SETTINGS use_skip_indexes_on_data_read = 1, query_plan_text_index_add_hint = 1
)
WHERE explain LIKE '%Filter column:%';

SELECT count() FROM tab WHERE hasToken(col, 'config');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(col, 'config')
    SETTINGS use_skip_indexes_on_data_read = 1, query_plan_text_index_add_hint = 1
)
WHERE explain LIKE '%Filter column:%';

DROP TABLE tab;
