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
SETTINGS index_granularity = 64;

-- Total 156 ranges
INSERT INTO tab1 SELECT number+1, number+1, (10000 - number), (number * 5) FROM numbers(10000);

SET use_skip_indexes = 1;
SET use_skip_indexes_on_disjuncts = 0;

-- 156
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%';

-- 78
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (id >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- 78
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (_part_offset >= 5000 AND (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- 156
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (id = 5000 OR (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- 156 (No skip index on v3)
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%';

SET use_skip_indexes_on_disjuncts = 1;

SELECT 'Now using the disjunct processing';

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

-- Final 3
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (id = 5000 OR (v1 = 111 OR v2 = 111))
) WHERE explain LIKE '%Granules%';

-- 156 (No skip index on v3)
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%';
