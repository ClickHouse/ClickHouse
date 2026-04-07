-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET use_statistics = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

-- Tests text search setting 'query_plan_text_index_add_hint' with different tokenizers

DROP TABLE IF EXISTS tab;

SELECT '-- splitByNonAlpha';

CREATE TABLE tab
(
    s String,
    INDEX idx (s) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number FROM numbers(100000);

SELECT count() FROM tab WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1, query_plan_remove_unused_columns = 1 -- CI may inject False; unused text index hint columns not pruned → extra INPUT entries in EXPLAIN actions output
) WHERE explain LIKE '%INPUT%\_\_text_index%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1, query_plan_remove_unused_columns = 1 -- CI may inject False; unused text index hint columns not pruned → extra INPUT entries in EXPLAIN actions output
) WHERE explain LIKE '%INPUT%\_\_text_index%';

DROP TABLE tab;

SELECT '-- array';

CREATE TABLE tab
(
    s String,
    INDEX idx (s) TYPE text(tokenizer = array) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number FROM numbers(100000);

SELECT count() FROM tab WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1, query_plan_remove_unused_columns = 1 -- CI may inject False; unused text index hint columns not pruned → extra INPUT entries in EXPLAIN actions output
) WHERE explain LIKE '%INPUT%\_\_text_index%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1, query_plan_remove_unused_columns = 1 -- CI may inject False; unused text index hint columns not pruned → extra INPUT entries in EXPLAIN actions output
) WHERE explain LIKE '%INPUT%\_\_text_index%';

DROP TABLE tab;

SELECT '-- ngrams(3)';

CREATE TABLE tab
(
    s String,
    INDEX idx (s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number FROM numbers(100000);

SELECT count() FROM tab WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1, query_plan_remove_unused_columns = 1 -- CI may inject False; unused text index hint columns not pruned → extra INPUT entries in EXPLAIN actions output
) WHERE explain LIKE '%INPUT%\_\_text_index%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1, query_plan_remove_unused_columns = 1 -- CI may inject False; unused text index hint columns not pruned → extra INPUT entries in EXPLAIN actions output
) WHERE explain LIKE '%INPUT%\_\_text_index%';

DROP TABLE tab;
