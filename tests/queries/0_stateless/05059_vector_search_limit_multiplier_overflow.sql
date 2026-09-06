-- Tags: no-fasttest, no-ordinary-database

-- With additional filters (or rescoring), the vector similarity index fetches LIMIT * vector_search_index_fetch_multiplier
-- neighbours. For a LIMIT close to the maximum of UInt64 the product exceeds the range of size_t, and converting it
-- was undefined behavior (reported by UBSan). The product is now capped by max_limit_for_vector_search_queries before
-- the conversion; the resulting huge neighbour count is then rejected by the index, as without the multiplier.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
WHERE id > 0
ORDER BY L2Distance(vec, reference_vec)
LIMIT 9223372036854775807
SETTINGS max_limit_for_vector_search_queries = 9223372036854775807, vector_search_index_fetch_multiplier = 2.0; -- { serverError INCORRECT_DATA }

WITH [0.0, 2.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 9223372036854775807
SETTINGS max_limit_for_vector_search_queries = 9223372036854775807, vector_search_index_fetch_multiplier = 2.0, vector_search_with_rescoring = 1; -- { serverError INCORRECT_DATA }

DROP TABLE tab;
