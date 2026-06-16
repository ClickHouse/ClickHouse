-- Tags: no-fasttest

-- Testing the vectorSearch table function

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [1.1, 0.0, 0.0]), (2, [0.0, 2.0, 0.0]), (3, [0.0, 0.0, 3.0]), (4, [0.5, 0.5, 0.5]);

SELECT '-- 1. Basic KNN search: top 3 nearest to [1.0, 0.0, 0.0]';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0, 0.0], 3)
ORDER BY _score, id;

SELECT '-- 1v. Verification via ORDER BY L2Distance LIMIT';
SELECT id, L2Distance(vec, [1.0, 0.0, 0.0]) AS _score
FROM tab
ORDER BY _score, id
LIMIT 3;

SELECT '-- 2. All columns are returned';
SELECT id, vec, _score, _part
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0, 0.0], 2)
ORDER BY _score, id;

SELECT '-- 3. K=1: return single nearest';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 2.0, 0.0], 1)
ORDER BY _score;

SELECT '-- 3v. Verification via ORDER BY L2Distance LIMIT';
SELECT id, L2Distance(vec, [0.0, 2.0, 0.0]) AS _score
FROM tab
ORDER BY _score
LIMIT 1;

DROP TABLE tab;

SELECT '-- 4. Multi-part test: results merged across parts';

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- Insert into separate parts.
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);
INSERT INTO tab VALUES (2, [0.0, 1.0]), (3, [0.0, 1.1]);

SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
ORDER BY _score, id;

SELECT '-- 4v. Verification via ORDER BY L2Distance LIMIT';
SELECT id, L2Distance(vec, [1.0, 0.0]) AS _score
FROM tab
ORDER BY _score, id
LIMIT 3;

SELECT '-- 5. Part name filtering';
SELECT id, _score, _part
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 10)
WHERE _part = 'all_1_1_0'
ORDER BY _score, id;

DROP TABLE tab;

SELECT '-- 5b. Partition pruning';

CREATE TABLE tab
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

SYSTEM STOP MERGES tab;

-- Insert into separate partitions.
INSERT INTO tab VALUES (0, 'a', [1.0, 0.0]), (1, 'a', [1.1, 0.0]);
INSERT INTO tab VALUES (2, 'b', [0.0, 1.0]), (3, 'b', [0.0, 1.1]);

-- Without partition filter: search across all partitions.
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
ORDER BY _score, id;

-- With partition filter: only search in partition 'a'.
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE category = 'a'
ORDER BY _score, id;

DROP TABLE tab;

SELECT '-- 6. Larger dataset with cosineDistance';

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 3) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT number, [toFloat32(cityHash64(number * 3 + 1) % 1000000 / 1000000.0), toFloat32(cityHash64(number * 3 + 2) % 1000000 / 1000000.0), toFloat32(cityHash64(number * 3 + 3) % 1000000 / 1000000.0)] FROM numbers(1000);

SELECT '-- 6a. Table function results';
SELECT id, _score
FROM vectorSearch(currentDatabase(), tab, idx, [0.5, 0.5, 0.5], 10)
ORDER BY _score, id;

SELECT '-- 6v. Verification via ORDER BY cosineDistance LIMIT';
SELECT id, cosineDistance(vec, [0.5, 0.5, 0.5]) AS _score
FROM tab
ORDER BY _score, id
LIMIT 10;

DROP TABLE tab;

SELECT '-- 7. Error: non-existent index';
CREATE TABLE tab (id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
SELECT * FROM vectorSearch(currentDatabase(), tab, no_such_index, [1.0], 1); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab;

SELECT '-- 8. Error: wrong index type';
CREATE TABLE tab (id Int32, s String, INDEX idx s TYPE minmax) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
SELECT * FROM vectorSearch(currentDatabase(), tab, idx, [1.0], 1); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab;
