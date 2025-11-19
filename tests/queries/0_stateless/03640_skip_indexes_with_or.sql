-- Tests for skip index with predicate conditions containing AND and OR.

DROP TABLE IF EXISTS tab1;

CREATE TABLE tab1
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
SETTINGS index_granularity = 64,min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

-- Total 157 ranges
INSERT INTO tab1 SELECT number+1, number+1, (10000 - number), (number * 5) FROM numbers(10000);

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes_on_disjuncts = 0;
SET use_query_condition_cache = 0; -- explain plan stability

-- 157
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%';

-- 79
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (id >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- 79
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (_part_offset >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- 157 (No skip index on v3)
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%';

SELECT 'Now using the disjunct processing';
SET use_skip_indexes_on_disjuncts = 1;

-- Final 2
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%';

-- Final 1
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (id >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- Final 1
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (_part_offset >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- Final 157 (because no skip index on v3)
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%';

-- Complex condition, 0 granules selected
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (v1 BETWEEN 10 AND 20 AND v2 BETWEEN 10 AND 20) OR (v1 BETWEEN 100 AND 2000 AND v2 BETWEEN 100 AND 2000) OR (v1 > 9000 AND v2 > 9000)
) WHERE explain LIKE '%Granules%';

-- Test with RPN size of 23 - only 6 granules and 6x64=384 rows should be read
SELECT count(*) FROM tab1 WHERE  (v1 = 1 AND v2 = 10000) OR (v1 = 129 AND v2 = 9872) OR (v1 = 999 OR v2 = 9002) OR (v1 = 1300 AND v2 = 8701) OR (v1 = 5000 AND v2 = 5001) OR (v1 = 9000 AND v2 = 1001) SETTINGS max_rows_to_read=384;

DROP TABLE tab1;

DROP TABLE IF EXISTS tab2;

-- Test with composite primary key condition
CREATE TABLE tab2
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
ORDER BY (x,y)
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab2 SELECT (number+1)/10, (number+1)%100, number+1, (10000 - number), (number * 5) FROM numbers(1000);

-- Final 1
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT x,y,v1,v2 FROM tab2 WHERE (x < 100 AND y < 20) AND (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%';

DROP TABLE tab2;
