-- Tags: no-fasttest, no-ordinary-database

-- Tests various scalar quantization for vector similarity indexes with i8 quantization.
-- The effect of quantization is extremely subtle and hard to test, so we are only testing the related settings.

SET allow_experimental_vector_similarity_index = 1;

SET enable_analyzer = 0;

DROP TABLE IF EXISTS tab;

-- Quantization interval invalid

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i8', 0, 0) GRANULARITY 4) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 5, scalar_quantization_quantile_for_vector_similarity_index = 1.1;
INSERT INTO tab VALUES (0, [1.0, 2.0]), (1, [1.1, 2.1]), (2, [1.2, 2.2]), (3, [1.3, 2.3]), (4, [1.4, 2.4]), (5, [1.5, 2.5]), (6, [1.6, 2.6]), (7, [1.7, 2.7]), (8, [1.8, 2.8]), (9, [1.9, 2.9]), (10, [1.0, 2.0]), (11, [1.1, 2.1]), (12, [1.2, 2.2]), (13, [1.3, 2.3]), (14, [1.4, 2.4]), (15, [1.5, 2.5]), (16, [1.6, 2.6]), (17, [1.7, 2.7]), (18, [1.8, 2.8]), (19, [1.9, 2.9]); -- { serverError INVALID_SETTING_VALUE }

DROP TABLE tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i8', 0, 0) GRANULARITY 4) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 5, scalar_quantization_quantile_for_vector_similarity_index = 0.4;
INSERT INTO tab VALUES (0, [1.0, 2.0]), (1, [1.1, 2.1]), (2, [1.2, 2.2]), (3, [1.3, 2.3]), (4, [1.4, 2.4]), (5, [1.5, 2.5]), (6, [1.6, 2.6]), (7, [1.7, 2.7]), (8, [1.8, 2.8]), (9, [1.9, 2.9]), (10, [1.0, 2.0]), (11, [1.1, 2.1]), (12, [1.2, 2.2]), (13, [1.3, 2.3]), (14, [1.4, 2.4]), (15, [1.5, 2.5]), (16, [1.6, 2.6]), (17, [1.7, 2.7]), (18, [1.8, 2.8]), (19, [1.9, 2.9]); -- { serverError INVALID_SETTING_VALUE }

DROP TABLE tab;

-- Test that no bad things happen
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 'i8', 0, 0) GRANULARITY 4) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 5, scalar_quantization_quantile_for_vector_similarity_index = 0.9;
INSERT INTO tab VALUES (0, [1.0, 2.0]), (1, [1.1, 2.1]), (2, [1.2, 2.2]), (3, [1.3, 2.3]), (4, [1.4, 2.4]), (5, [1.5, 2.5]), (6, [1.6, 2.6]), (7, [1.7, 2.7]), (8, [1.8, 2.8]), (9, [1.9, 2.9]), (10, [1.0, 2.0]), (11, [1.1, 2.1]), (12, [1.2, 2.2]), (13, [1.3, 2.3]), (14, [1.4, 2.4]), (15, [1.5, 2.5]), (16, [1.6, 2.6]), (17, [1.7, 2.7]), (18, [1.8, 2.8]), (19, [1.9, 2.9]);

WITH [1.4, 2.4] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec), id
LIMIT 3;

DROP TABLE tab;
