-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET use_statistics = 0;

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
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

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
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

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
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s = '5555' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM tab WHERE s LIKE '%5555%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE s LIKE '%5555%' SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%filter column%';

DROP TABLE tab;
