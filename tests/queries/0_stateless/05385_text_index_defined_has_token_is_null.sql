-- Tags: no-parallel-replicas, no-random-merge-tree-settings

SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS text_index_defined_has_token_is_null;

CREATE TABLE text_index_defined_has_token_is_null
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_has_token_is_null;

INSERT INTO text_index_defined_has_token_is_null VALUES
    (0, 'alpha'), (1, 'alpha'), (2, 'alpha'), (3, 'alpha'),
    (4, 'alpha'), (5, 'alpha'), (6, 'alpha'), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, NULL), (11, NULL),
    (12, NULL), (13, NULL), (14, NULL), (15, NULL),
    (16, 'beta');

SELECT 'scan hasToken IS NULL';
SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NULL ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'indexed hasToken IS NULL';
SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NULL ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'scan isNull(hasToken)';
SELECT id FROM text_index_defined_has_token_is_null WHERE isNull(hasToken(value, 'alpha')) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'indexed isNull(hasToken)';
SELECT id FROM text_index_defined_has_token_is_null WHERE isNull(hasToken(value, 'alpha')) ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'scan missing-token IS NULL';
SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'zzz') IS NULL ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'indexed missing-token IS NULL';
SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'zzz') IS NULL ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'scan hasToken IS NOT NULL';
SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NOT NULL ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'indexed hasToken IS NOT NULL';
SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NOT NULL ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'scan isNotNull(hasToken)';
SELECT id FROM text_index_defined_has_token_is_null WHERE isNotNull(hasToken(value, 'alpha')) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'indexed isNotNull(hasToken)';
SELECT id FROM text_index_defined_has_token_is_null WHERE isNotNull(hasToken(value, 'alpha')) ORDER BY id
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'hasToken IS NULL skips the all-defined mark';
SELECT countIf(explain LIKE '%Name: i%') > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NULL
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NULL
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

SELECT 'isNull(hasToken) skips the all-defined mark';
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_has_token_is_null WHERE isNull(hasToken(value, 'alpha'))
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

SELECT 'missing-token IS NULL skips the all-defined mark';
SELECT countIf(explain LIKE '%Granules: 1/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'zzz') IS NULL
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

SELECT 'hasToken IS NOT NULL skips the all-NULL mark';
SELECT countIf(explain LIKE '%Granules: 2/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_has_token_is_null WHERE hasToken(value, 'alpha') IS NOT NULL
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

SELECT 'hasTokenOrNull IS NULL does not use defined skip';
SELECT countIf(explain LIKE '%Granules: 3/3%')
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM text_index_defined_has_token_is_null WHERE hasTokenOrNull(value, 'alpha') IS NULL
    SETTINGS
        use_skip_indexes = 1,
        use_skip_indexes_on_data_read = 0,
        merge_tree_min_rows_for_seek = 0,
        merge_tree_min_bytes_for_seek = 0
);

DROP TABLE text_index_defined_has_token_is_null;
