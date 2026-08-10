-- Tags: no-fasttest

-- Per plan 02 §step 2.5: vectorSearch has two arities:
--   - 5-arg: vectorSearch(db, table, index, [ref], K) — explicit index.
--   - 4-arg: vectorSearch(db, table, [ref], K) — auto-resolved, the
--     table must have exactly one vector_similarity index.
--
-- Failure modes per the parser:
--   * 5-arg with a non-existent index name → BAD_ARGUMENTS.
--   * 5-arg with a non-vector-similarity index → BAD_ARGUMENTS.
--   * 4-arg with zero vector_similarity indexes → BAD_ARGUMENTS.
--   * 4-arg with two-or-more vector_similarity indexes → BAD_ARGUMENTS.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS one_idx;
DROP TABLE IF EXISTS two_idx;
DROP TABLE IF EXISTS no_idx;
DROP TABLE IF EXISTS mixed_idx;

CREATE TABLE one_idx(id Int32, vec Array(Float32), INDEX idx_a vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO one_idx VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);

CREATE TABLE two_idx(id Int32, vec Array(Float32), INDEX idx_a vec TYPE vector_similarity('hnsw', 'L2Distance', 2), INDEX idx_b vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO two_idx VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);

CREATE TABLE no_idx(id Int32, vec Array(Float32))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO no_idx VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);

CREATE TABLE mixed_idx(id Int32, vec Array(Float32), s String, INDEX idx_str s TYPE minmax, INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO mixed_idx VALUES (0, [1.0, 0.0], 'a'), (1, [1.1, 0.0], 'b');

SELECT '-- 5-arg explicit index works';
SELECT id FROM vectorSearch(currentDatabase(), one_idx, idx_a, [1.0, 0.0], 2) ORDER BY _score, id;

SELECT '-- 4-arg auto-resolve when exactly one vector_similarity index';
SELECT id FROM vectorSearch(currentDatabase(), one_idx, [1.0, 0.0], 2) ORDER BY _score, id;

SELECT '-- 4-arg with mixed index types (only one is vector_similarity) auto-resolves';
SELECT id FROM vectorSearch(currentDatabase(), mixed_idx, [1.0, 0.0], 2) ORDER BY _score, id;

SELECT '-- 5-arg with non-existent index name → BAD_ARGUMENTS';
SELECT id FROM vectorSearch(currentDatabase(), one_idx, nonexistent_idx, [1.0, 0.0], 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- 5-arg with a non-vector index → BAD_ARGUMENTS';
SELECT id FROM vectorSearch(currentDatabase(), mixed_idx, idx_str, [1.0, 0.0], 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- 4-arg with zero vector_similarity indexes → BAD_ARGUMENTS';
SELECT id FROM vectorSearch(currentDatabase(), no_idx, [1.0, 0.0], 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- 4-arg with two vector_similarity indexes → BAD_ARGUMENTS';
SELECT id FROM vectorSearch(currentDatabase(), two_idx, [1.0, 0.0], 2); -- { serverError BAD_ARGUMENTS }

DROP TABLE one_idx;
DROP TABLE two_idx;
DROP TABLE no_idx;
DROP TABLE mixed_idx;
