-- Tags: no-parallel-replicas, no-random-merge-tree-settings

SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS text_index_defined_mark_range;

CREATE TABLE text_index_defined_mark_range
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_mark_range;

INSERT INTO text_index_defined_mark_range VALUES
    (0, 'alpha'), (1, 'alpha'), (2, 'alpha'), (3, 'alpha'),
    (4, 'alpha'), (5, 'alpha'), (6, 'alpha'), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, NULL), (11, NULL),
    (12, NULL), (13, NULL), (14, NULL), (15, NULL),
    (16, 'beta');

SELECT 'oracle isNull';
SELECT id FROM text_index_defined_mark_range WHERE isNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'oracle isNotNull';
SELECT id FROM text_index_defined_mark_range WHERE isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'indexed isNull';
SELECT id FROM text_index_defined_mark_range WHERE isNull(value) ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'indexed isNotNull';
SELECT id FROM text_index_defined_mark_range WHERE isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'isNull skips the all-defined mark';
SELECT countIf(explain LIKE '%Name: i%') > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_mark_range WHERE isNull(value)
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);
-- Clipped last granule is all-defined, so `isNull` keeps only the all-NULL mark.
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_mark_range WHERE isNull(value)
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

SELECT 'isNotNull skips the all-NULL mark';
SELECT countIf(explain LIKE '%Granules: 2/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_mark_range WHERE isNotNull(value)
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

SELECT 'hasToken skips the all-NULL mark';
-- Token is only in the first mark; the clipped tail mark has `beta`.
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_mark_range WHERE hasToken(value, 'alpha')
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

DROP TABLE text_index_defined_mark_range;
