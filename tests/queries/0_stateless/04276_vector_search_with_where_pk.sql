-- Tags: no-fasttest

-- Per plan 02 §step 5: WHERE clauses on vectorSearch compose with the
-- usual MergeTree PK pruning. PK-prunable predicates filter granules
-- before the bitmap subquery runs, and the prefilter is the
-- intersection with the in-range rows.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO tab VALUES
    (0, [1.0, 0.0]),
    (1, [1.1, 0.0]),
    (2, [1.2, 0.0]),
    (3, [1.3, 0.0]),
    (4, [0.0, 2.0]),
    (5, [0.0, 2.1]),
    (6, [0.0, 2.2]),
    (7, [0.0, 2.3]);

SELECT '-- Top-K with no WHERE: pre-filter is null';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
ORDER BY _score, id;

SELECT '-- Top-K with WHERE id < 4: PK-prunable to first granule';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE id < 4
ORDER BY _score, id;

SELECT '-- Top-K with WHERE id >= 4: PK-prunable to second half';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE id >= 4
ORDER BY _score, id;

SELECT '-- Top-K with WHERE id IN (1, 3, 5, 7): non-contiguous PK filter';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE id IN (1, 3, 5, 7)
ORDER BY _score, id;

SELECT '-- Top-K with WHERE id BETWEEN 2 AND 5';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE id BETWEEN 2 AND 5
ORDER BY _score, id;

DROP TABLE tab;
