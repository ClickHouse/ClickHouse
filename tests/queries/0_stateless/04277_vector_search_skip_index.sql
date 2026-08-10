-- Tags: no-fasttest

-- Per plan 02 §step 5: WHERE clauses on vectorSearch compose with
-- secondary skip indexes (minmax / bloom_filter) the same way as a
-- normal MergeTree query. The skip indexes prune granules; the
-- bitmap subquery only ever scans the surviving granules.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    category UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000,
    INDEX cat_idx category TYPE minmax GRANULARITY 1,
    INDEX cat_bloom category TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES
    (0, 10, [1.0, 0.0]),
    (1, 10, [1.1, 0.0]),
    (2, 10, [1.2, 0.0]),
    (3, 10, [1.3, 0.0]),
    (4, 20, [0.0, 2.0]),
    (5, 20, [0.0, 2.1]),
    (6, 20, [0.0, 2.2]),
    (7, 20, [0.0, 2.3]);

SELECT '-- minmax skip prunes second granule (category = 10)';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE category = 10
ORDER BY _score, id;

SELECT '-- minmax skip prunes first granule (category = 20)';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE category = 20
ORDER BY _score, id;

SELECT '-- bloom_filter equivalent (category IN list)';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 5)
WHERE category IN (10, 999)
ORDER BY _score, id;

SELECT '-- category = 30 prunes everything → 0 rows';
SELECT count()
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE category = 30;

DROP TABLE tab;
