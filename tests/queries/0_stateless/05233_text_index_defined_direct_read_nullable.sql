-- Tags: no-parallel-replicas, no-random-merge-tree-settings

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS text_index_defined_direct_read;

CREATE TABLE text_index_defined_direct_read
(
    id UInt32,
    value Nullable(String),
    INDEX value_index value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_direct_read;

INSERT INTO text_index_defined_direct_read VALUES
    (0, 'alpha beta'), (1, 'alpha'), (2, 'alpha'), (3, 'alpha'),
    (4, 'alpha'), (5, 'alpha'), (6, 'alpha'), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, NULL), (11, NULL),
    (12, NULL), (13, NULL), (14, NULL), (15, NULL),
    (16, '');

SELECT 'indexed';
SELECT id, value, hasToken(value, 'alpha'), isNull(value) FROM text_index_defined_direct_read ORDER BY id;
SELECT id FROM text_index_defined_direct_read WHERE hasToken(value, 'alpha') ORDER BY id;
SELECT id FROM text_index_defined_direct_read WHERE isNull(value) ORDER BY id;
SELECT id FROM text_index_defined_direct_read WHERE isNotNull(value) ORDER BY id;
SELECT id FROM text_index_defined_direct_read WHERE NOT hasToken(value, 'alpha') ORDER BY id;

SELECT 'scan';
SELECT id, value, hasToken(value, 'alpha'), isNull(value) FROM text_index_defined_direct_read ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_direct_read WHERE hasToken(value, 'alpha') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_direct_read WHERE isNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_direct_read WHERE isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_direct_read WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'explain';
SELECT countIf(explain LIKE '%Name: value_index%') > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_direct_read WHERE isNull(value)
    SETTINGS use_skip_indexes_on_data_read = 0, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0
);
-- Last granule is clipped to the part row count, so the all-defined / all-matching
-- tail mark is skipped: `isNull` keeps only the all-NULL mark, `NOT hasToken` keeps
-- only the empty-string mark.
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_direct_read WHERE isNull(value)
    SETTINGS use_skip_indexes_on_data_read = 0, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0
);
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_direct_read WHERE NOT hasToken(value, 'alpha')
    SETTINGS use_skip_indexes_on_data_read = 0, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0
);

DROP TABLE text_index_defined_direct_read;
