-- Tags: no-fasttest

-- Per plan 02 §"Coverage matrix" row "Base invariants": vectorSearch
-- composes with the MergeTree base in the same way every reader does:
-- PARTITION BY, ORDER BY, and the part-level skip-by-statistics path
-- all see the table function as a normal SELECT.

SET allow_experimental_search_topk_table_functions = 1;

-- 1. PARTITION BY prunes both for direct SELECTs and for vectorSearch.

DROP TABLE IF EXISTS part_tab;

CREATE TABLE part_tab
(
    id Int32,
    category String,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
PARTITION BY category
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO part_tab VALUES (0, 'a', [1.0, 0.0]), (1, 'a', [1.1, 0.0]);
INSERT INTO part_tab VALUES (2, 'b', [0.0, 1.0]), (3, 'b', [0.0, 1.1]);
INSERT INTO part_tab VALUES (4, 'c', [2.0, 2.0]), (5, 'c', [2.1, 2.1]);

SELECT '-- Partition prune: WHERE category = a';
SELECT id FROM vectorSearch(currentDatabase(), part_tab, idx, [1.0, 0.0], 5) WHERE category = 'a' ORDER BY _score, id;

SELECT '-- Partition prune: WHERE category IN (a, c)';
SELECT id FROM vectorSearch(currentDatabase(), part_tab, idx, [1.0, 0.0], 5) WHERE category IN ('a', 'c') ORDER BY _score, id;

SELECT '-- Partition prune: WHERE category = nonexistent';
SELECT count() FROM vectorSearch(currentDatabase(), part_tab, idx, [1.0, 0.0], 5) WHERE category = 'd';

DROP TABLE part_tab;

-- 2. ORDER BY-keyed PK pruning composes with vectorSearch (similar to
-- 04276 but with a compound key).

DROP TABLE IF EXISTS pk_tab;

CREATE TABLE pk_tab
(
    a Int32,
    b Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 4;

INSERT INTO pk_tab VALUES (1, 10, [1.0, 0.0]), (1, 20, [1.1, 0.0]), (2, 10, [0.0, 1.0]), (2, 20, [0.0, 1.1]);

SELECT '-- Compound PK prune: WHERE a = 1';
SELECT a, b FROM vectorSearch(currentDatabase(), pk_tab, idx, [1.0, 0.0], 4) WHERE a = 1 ORDER BY _score, a, b;

SELECT '-- Compound PK prune: WHERE (a, b) = (2, 10)';
SELECT a, b FROM vectorSearch(currentDatabase(), pk_tab, idx, [1.0, 0.0], 4) WHERE (a, b) = (2, 10) ORDER BY _score, a, b;

DROP TABLE pk_tab;

-- 3. Column statistics on a non-vec column don't interfere.

DROP TABLE IF EXISTS stat_tab;

CREATE TABLE stat_tab
(
    id Int32,
    val Float64 STATISTICS(tdigest),
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_statistics = 1, allow_statistics_optimize = 1;

INSERT INTO stat_tab VALUES (0, 0.5, [1.0, 0.0]), (1, 1.5, [1.1, 0.0]), (2, 0.7, [0.0, 1.0]), (3, 0.3, [0.0, 1.1]);

SELECT '-- Statistics column does not interfere with vectorSearch';
SELECT id FROM vectorSearch(currentDatabase(), stat_tab, idx, [1.0, 0.0], 2) ORDER BY _score, id;

SELECT '-- Statistics-aware WHERE composes with vectorSearch';
SELECT id FROM vectorSearch(currentDatabase(), stat_tab, idx, [1.0, 0.0], 4) WHERE val > 0.5 ORDER BY _score, id;

DROP TABLE stat_tab;
