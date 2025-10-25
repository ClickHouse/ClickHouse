-- Tests for skip index with predicate conditions containing AND and OR.
-- Tags: no-random-settings, no-random-merge-tree-settings

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
SET use_skip_indexes_on_data_read = 0;
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

-- 156 (No skip index on v3)
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

-- Final 156 (because no skip index on v3)
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE v1 = 111 OR v2 = 111 OR v3 = 90000
) WHERE explain LIKE '%Granules%';

-- Complex condition, 0 granules selected
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab1 WHERE (v1 BETWEEN 10 AND 20 AND v2 BETWEEN 10 AND 20) OR (v1 BETWEEN 100 AND 2000 AND v2 BETWEEN 100 AND 2000) OR (v1 > 9000 AND v2 > 9000)
) WHERE explain LIKE '%Granules%';

DROP TABLE tab1;

DROP TABLE IF EXISTS tab2;

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
SETTINGS index_granularity = 100;

INSERT INTO tab2 SELECT (number+1)/10, (number+1)%100, number+1, (10000 - number), (number * 5) FROM numbers(1000);

-- Final 1
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT x,y,v1,v2 FROM tab2 WHERE (x < 100 AND y < 20) AND (v1 = 111 OR v2 = 111)
) WHERE explain LIKE '%Granules%';

DROP TABLE tab2;
