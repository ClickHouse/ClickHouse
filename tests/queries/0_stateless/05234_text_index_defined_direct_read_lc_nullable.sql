-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- `LowCardinality(Nullable(String))` must use the text index defined virtual column for `isNull` / `isNotNull`.
-- `DataTypeLowCardinality::isNullable` is false, so the defined atom has to accept `isLowCardinalityNullable`.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET short_circuit_function_evaluation_for_nulls = 0;

DROP TABLE IF EXISTS text_index_defined_lc_nullable;

CREATE TABLE text_index_defined_lc_nullable
(
    id UInt32,
    value LowCardinality(Nullable(String)),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_lc_nullable;

INSERT INTO text_index_defined_lc_nullable VALUES
    (0, 'alpha beta'), (1, 'alpha'), (2, 'alpha'), (3, 'alpha'),
    (4, 'alpha'), (5, 'alpha'), (6, 'alpha'), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, NULL), (11, NULL),
    (12, NULL), (13, NULL), (14, NULL), (15, NULL),
    (16, '');

SELECT countIf(explain LIKE '%__text_index_i_isNull_%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM text_index_defined_lc_nullable WHERE isNull(value) ORDER BY id);

SELECT countIf(explain LIKE '%__text_index_i_isNotNull_%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM text_index_defined_lc_nullable WHERE isNotNull(value) ORDER BY id);

SELECT 'indexed';
SELECT id FROM text_index_defined_lc_nullable WHERE isNull(value) ORDER BY id;
SELECT id FROM text_index_defined_lc_nullable WHERE isNotNull(value) ORDER BY id;
SELECT id FROM text_index_defined_lc_nullable WHERE hasToken(value, 'alpha') AND isNotNull(value) ORDER BY id;
SELECT id FROM text_index_defined_lc_nullable WHERE NOT hasToken(value, 'alpha') ORDER BY id;

SELECT 'scan';
SELECT id FROM text_index_defined_lc_nullable WHERE isNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_lc_nullable WHERE isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_lc_nullable WHERE hasToken(value, 'alpha') AND isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_lc_nullable WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE text_index_defined_lc_nullable;
