-- Tags: no-fasttest, no-ordinary-database

-- Coverage for the 'e8' fastknn quantization (cosineDistance and L2Distance), the experimental rollout gate
-- (allow_experimental_fastknn_index), and validation boundaries for the flat-only quantizations.

SET enable_analyzer = 1;
SET allow_experimental_vector_similarity_index = 1;
SET allow_experimental_fastknn_index = 1;

-- ---------------------------------------------------------------------------------------------------------------------
-- Experimental rollout gate: 'fastknn' is rejected unless allow_experimental_fastknn_index is set.
SET allow_experimental_fastknn_index = 0;
CREATE TABLE tab_gate
(
    id UInt32,
    vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'cosineDistance', 16, 'e8', 8, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
SET allow_experimental_fastknn_index = 1;

-- ---------------------------------------------------------------------------------------------------------------------
-- 'e8' functional coverage for cosineDistance and L2Distance.
DROP TABLE IF EXISTS tab_e8_cos;
DROP TABLE IF EXISTS tab_e8_l2;

CREATE TABLE tab_e8_cos
(
    id UInt32,
    vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'cosineDistance', 16, 'e8', 8, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_e8_l2
(
    id UInt32,
    vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'L2Distance', 16, 'e8', 8, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_e8_cos SELECT number, arrayMap(i -> toFloat32(sin(number * 16 + i)), range(16)) FROM numbers(64);
INSERT INTO tab_e8_l2 SELECT * FROM tab_e8_cos;

-- Rescoring is on so the returned distance is exact and platform-independent (the raw e8 ranking differs slightly
-- between the scalar ADC and the AVX-512 fast scan, so we do not assert on it directly).
SELECT '-- e8 cosineDistance: nearest neighbour of a row is itself';
SELECT id FROM tab_e8_cos ORDER BY cosineDistance(vec, (SELECT vec FROM tab_e8_cos WHERE id = 7)) LIMIT 1 SETTINGS vector_search_with_rescoring = 1;

SELECT '-- e8 L2Distance: nearest neighbour of a row is itself';
SELECT id FROM tab_e8_l2 ORDER BY L2Distance(vec, (SELECT vec FROM tab_e8_l2 WHERE id = 7)) LIMIT 1 SETTINGS vector_search_with_rescoring = 1;

SELECT '-- e8 returns the requested number of rows and uses the index';
SELECT count() FROM (SELECT id FROM tab_e8_cos ORDER BY cosineDistance(vec, (SELECT vec FROM tab_e8_cos WHERE id = 7)) LIMIT 5);
SELECT countIf(explain LIKE '%vec_idx%') > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM tab_e8_cos ORDER BY cosineDistance(vec, (SELECT vec FROM tab_e8_cos WHERE id = 7)) LIMIT 5);

DROP TABLE tab_e8_cos;
DROP TABLE tab_e8_l2;

-- ---------------------------------------------------------------------------------------------------------------------
-- 'e8' validation boundaries (dimension multiple of 8, bits in 1..16, supported metric, bounded ADC table).
CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'cosineDistance', 12, 'e8', 8, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'cosineDistance', 16, 'e8', 0, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'cosineDistance', 16, 'e8', 17, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'dotProduct', 16, 'e8', 8, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

-- ADC lookup table too large (high dimension x high bits).
CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('fastknn', 'cosineDistance', 65536, 'e8', 16, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

-- ---------------------------------------------------------------------------------------------------------------------
-- Flat-only quantizations are rejected for the 'hnsw' method (which would silently ignore the projection / codes).
CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 16, 'b1_projected', 0, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 16, 'e8', 8, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

CREATE TABLE tab_bad (id UInt32, vec Array(Float32),
    INDEX vec_idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 16, 'turboquant', 0, 0) GRANULARITY 100000000
) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }
