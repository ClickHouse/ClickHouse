-- Tags: no-parallel-replicas
-- no-parallel-replicas: funny EXPLAIN PLAN output

-- Test that the skip indexes are utilized for AND and OR connected filter conditions
-- This test uses all the skip index types - minmax, set, bloom filter, text

-- Settings needed to achieve stable EXPLAIN PLAN output
SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    i Int32,
    s String,
    u UInt32,
    t1 String,
    t2 String,
    INDEX minmax_index i TYPE minmax,
    INDEX set_index s TYPE set(10),
    INDEX bf_index u TYPE bloom_filter (0.001),
    INDEX text_index1 t1 TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX text_index2 t2 TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 6, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    use_const_adaptive_granularity = 1;

-- 600 rows, 100 granules
INSERT INTO tab
    SELECT number,
           number,
           IF (number < 6, 'firststring', IF(number < 588, 'middlestring', 'laststring')),
           number,
           concat('This is text in row number', toString(number)),
           concat('Some thing for line', toString(number))
    FROM numbers(600);

SELECT 'Test without utilizing skip indexes for disjunctions';
SET use_skip_indexes_for_disjunctions = 0;

SELECT '-- Simple OR condition'; -- surviving granules: 100, but only 1 granule is real match
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (i = 1 OR s = 'firststring' OR u = 1 OR hasToken(t1, 'number1'))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Mixed AND/OR condition'; -- will show 50 granules, but real match is 0 granules
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (id >= 301 AND (i = 1 OR s = 'firststring' OR u = 1 OR hasToken(t1, 'number1')))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';


-- Now test with feature enabled
SELECT 'Test with utilizing skip indexes for disjunctions';
SET use_skip_indexes_for_disjunctions = 1;

SELECT '-- Simple OR condition'; -- Should show 1 granule
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (i = 1 OR s = 'firststring' OR u = 1 OR hasToken(t1, 'number1'))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Mixed AND/OR condition'; -- final should be 0 granules
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (id >= 301 AND (i = 1 OR s = 'firststring' OR u = 1 OR hasToken(t1, 'number1')))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

-- Now test with mixing stuff
SELECT '-- Should show 3 granules, laststring is in the last 2 granules';
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (i = 10 OR s = 'laststring')
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Should show 1 granule';
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (hasToken(t1, 'number1') OR hasToken(t2, 'line1'))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Should show 2 granules';
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (hasToken(t1, 'number1') OR hasToken(t2, 'line85'))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';
