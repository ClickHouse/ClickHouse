-- Tags: no-parallel-replicas
-- no-parallel-replicas: funny EXPLAIN PLAN output

-- Test that the skip indexes are utilized for AND and OR connected filter conditions

-- Settings needed to achieve stable EXPLAIN PLAN output
SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v1 UInt32,
    v2 UInt32,
    v3 UInt32,
    INDEX v1_index v1 TYPE minmax,
    INDEX v2_index v2 TYPE minmax,
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    use_const_adaptive_granularity = 1;

-- 157 ranges in total
INSERT INTO tab SELECT number + 1, number + 1, (10000 - number), (number * 5) FROM numbers(10000);

SELECT 'Test without utilizing skip indexes for disjunctions';
SET use_skip_indexes_for_disjunctions = 0;

SELECT '-- Simple OR condition'; -- surviving granules: 159
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Mixed AND/OR condition'; -- 79
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (id >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Mixed _part_offset / OR condition'; -- 79
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (_part_offset >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- No skip index on v3'; -- 159
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT 'Test with utilizing skip indexes for disjunctions';
SET use_skip_indexes_for_disjunctions = 1;

SELECT '-- Simple OR condition'; -- 2
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Mixed AND/OR condition'; -- 1
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (id >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Mixed _part_offset / OR condition'; -- 1
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (_part_offset >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- No skip index on v3'; -- 157
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

SELECT '-- Complex condition'; -- 0
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE (v1 BETWEEN 10 AND 20 AND v2 BETWEEN 10 AND 20) OR (v1 BETWEEN 100 AND 2000 AND v2 BETWEEN 100 AND 2000) OR (v1 > 9000 AND v2 > 9000)
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

-- Test with RPN size of 23 - only 6 granules and 6x64=384 rows should be read
SELECT count(*) FROM tab WHERE  (v1 = 1 AND v2 = 10000) OR (v1 = 129 AND v2 = 9872) OR (v1 = 999 OR v2 = 9002) OR (v1 = 1300 AND v2 = 8701) OR (v1 = 5000 AND v2 = 5001) OR (v1 = 9000 AND v2 = 1001) SETTINGS max_rows_to_read=384;

DROP TABLE tab;

SELECT 'Test with composite primary key condition';
CREATE TABLE tab
(
    x UInt32,
    y UInt32,
    v1 UInt32,
    v2 UInt32,
    v3 UInt32,
    INDEX v1_index v1 TYPE minmax,
    INDEX v2_index v2 TYPE minmax,
)
ENGINE = MergeTree
ORDER BY (x, y)
SETTINGS
    index_granularity = 100, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    use_const_adaptive_granularity = 1;

INSERT INTO tab SELECT (number + 1) / 10, (number + 1) % 100, number + 1, (10000 - number), (number * 5) FROM numbers(1000);

-- 1
SELECT explain AS explain FROM (
    EXPLAIN indexes = 1 SELECT x, y, v1, v2 FROM tab WHERE (x < 100 AND y < 20) AND (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Name%';

DROP TABLE tab;
