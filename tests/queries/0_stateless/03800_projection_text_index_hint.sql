SET allow_experimental_projection_text_index = 1;
-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET use_statistics = 0;
SET query_plan_direct_read_from_text_index = 0;

-- Tests text search setting 'query_plan_text_index_add_hint' with different tokenizers

DROP TABLE IF EXISTS tab;

SELECT '-- splitByNonAlpha';

CREATE TABLE tab
(
    s String,
    PROJECTION idx INDEX s TYPE text(tokenizer = splitByNonAlpha)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number FROM numbers(100000);

SELECT count() FROM tab WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE tab;

SELECT '-- array';

CREATE TABLE tab
(
    s String,
    PROJECTION idx INDEX s TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number FROM numbers(100000);

SELECT count() FROM tab WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE tab;

SELECT '-- ngrams(3)';

CREATE TABLE tab
(
    s String,
    PROJECTION idx INDEX s TYPE text(tokenizer = ngrams(3))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT number FROM numbers(100000);

SELECT count() FROM tab WHERE s = '5555';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE tab;
