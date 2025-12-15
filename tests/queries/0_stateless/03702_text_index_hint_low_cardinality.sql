-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_direct_read_lc;
SET allow_experimental_full_text_index = 1;

CREATE OR REPLACE TABLE t_direct_read_lc
(
    c LowCardinality(String),
    INDEX i c type text(tokenizer='array')
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_direct_read_lc VALUES ('config');

SELECT count() FROM t_direct_read_lc WHERE c = 'config';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_direct_read_lc WHERE c = 'config'
    SETTINGS use_skip_indexes_on_data_read = 1, query_plan_text_index_add_hint = 1
)
WHERE explain LIKE '%Filter column:%';

SELECT count() FROM t_direct_read_lc WHERE hasToken(c, 'config');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_direct_read_lc WHERE hasToken(c, 'config')
    SETTINGS use_skip_indexes_on_data_read = 1, query_plan_text_index_add_hint = 1
)
WHERE explain LIKE '%Filter column:%';

DROP TABLE t_direct_read_lc;
