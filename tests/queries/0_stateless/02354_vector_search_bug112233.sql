-- Tags: no-fasttest, no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/112233
-- An integer reference vector such as [1, 2] denotes the same point as [1.0, 2.0], so it must use the index as well.

SET explain_query_plan_default = 'legacy';

SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES
  (0, [1.0, 0.0]),
  (1, [1.1, 0.0]),
  (2, [1.2, 0.0]),
  (3, [1.3, 0.0]),
  (4, [1.4, 0.0]),
  (5, [0.0, 2.0]),
  (6, [0.0, 2.1]),
  (7, [0.0, 2.2]),
  (8, [0.0, 2.3]),
  (9, [0.0, 2.4]);

SELECT '-- Unsigned integer reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0, 2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Signed integer reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [-1, -2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Mixed integer and float reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0, 2.0])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- BFloat16 reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0, 2]::Array(BFloat16))
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

-- Integer-to-float conversion is lossy in general, not only above some magnitude, so these tests only pin down that the
-- index is used.
SELECT '-- Reference vector above the Float32 mantissa: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [16777217, 2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Extremal integer reference vectors: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [4294967295, 2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [-2147483648, 2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Integer and float spellings of the same reference vector return the same result';
SELECT id FROM tab ORDER BY L2Distance(vec, [0, 2]) LIMIT 1;
SELECT id FROM tab ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 1;

DROP TABLE tab;

-- L2Distance and cosineDistance resolve a common type for their arguments, which rules out reference vectors whose
-- values the column's float type cannot represent exactly, e.g. [2^53 + 1, 2] against an Array(Float32) column
-- (NO_COMMON_TYPE), as well as reference vectors of a wider integer type. dotProduct has no such restriction, so it is
-- the operator that carries those reference vectors into this code path.

DROP TABLE IF EXISTS tab_dot;

CREATE TABLE tab_dot
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab_dot VALUES
  (0, [1.0, 0.0]),
  (1, [1.1, 0.0]),
  (2, [1.2, 0.0]),
  (3, [1.3, 0.0]),
  (4, [1.4, 0.0]),
  (5, [0.0, 2.0]),
  (6, [0.0, 2.1]),
  (7, [0.0, 2.2]),
  (8, [0.0, 2.3]),
  (9, [0.0, 2.4]);

-- 2^53 + 1 has no exact Float64 representation and is rounded down to 2^53 on conversion. Every native integer stays far
-- below `std::numeric_limits<Float64>::max` and inside the Float32 / BFloat16 range the index quantizes to, so no
-- reference vector element can turn into an infinity here.
SELECT '-- Reference vector above the Float64 mantissa: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab_dot
    ORDER BY dotProduct(vec, [9007199254740993, 2]) DESC
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';
SELECT id FROM tab_dot ORDER BY dotProduct(vec, [9007199254740993, 2]) DESC LIMIT 1;

SELECT '-- Extremal 64-bit integer reference vectors: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab_dot
    ORDER BY dotProduct(vec, [18446744073709551615, 2]) DESC
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';
SELECT id FROM tab_dot ORDER BY dotProduct(vec, [18446744073709551615, 2]) DESC LIMIT 1;
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab_dot
    ORDER BY dotProduct(vec, [-9223372036854775808, 2]) DESC
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';
SELECT id FROM tab_dot ORDER BY dotProduct(vec, [-9223372036854775808, 2]) DESC LIMIT 1;

SELECT '-- Integer and float spellings of a 64-bit reference vector return the same result';
SELECT id FROM tab_dot ORDER BY dotProduct(vec, [18446744073709551615, 2]) DESC LIMIT 1;
SELECT id FROM tab_dot ORDER BY dotProduct(vec, [18446744073709551615.0, 2.0]) DESC LIMIT 1;

DROP TABLE tab_dot;
