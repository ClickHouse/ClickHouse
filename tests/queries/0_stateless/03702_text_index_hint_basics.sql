-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET allow_statistics_optimize = 0;

DROP TABLE IF EXISTS t_text_index_hint;

SELECT 'splitByNonAlpha';

CREATE TABLE t_text_index_hint
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_hint SELECT number FROM numbers(100000);

SELECT count() FROM t_text_index_hint WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE IF EXISTS t_text_index_hint;

SELECT 'array';

CREATE TABLE t_text_index_hint
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = array) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_hint SELECT number FROM numbers(100000);

SELECT count() FROM t_text_index_hint WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE IF EXISTS t_text_index_hint;

SELECT 'ngrams(3)';

CREATE TABLE t_text_index_hint
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_hint SELECT number FROM numbers(100000);

SELECT count() FROM t_text_index_hint WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE IF EXISTS t_text_index_hint;
