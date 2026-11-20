-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Regression test for the vector-search `ORDER BY <distance> LIMIT` rewrite with a row policy.
--
-- A row policy restricts rows inside the reader, just like a `WHERE` or a `PREWHERE`, so it must count as an
-- additional filter. It did not, so a query filtered only by a policy was treated as unfiltered: an explicit
-- request for exact search via `vector_search_filter_strategy = 'prefilter'` was ignored, and the index
-- fetched only `LIMIT` neighbours without the `vector_search_index_fetch_multiplier` compensation, which the
-- policy can then discard.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP ROW POLICY IF EXISTS rp_04814 ON t_04814;
DROP TABLE IF EXISTS t_04814;

CREATE TABLE t_04814 (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04814 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

SELECT 'no policy, prefilter: uses the index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'prefilter')
WHERE explain LIKE '%vector_similarity%';

CREATE ROW POLICY rp_04814 ON t_04814 FOR SELECT USING id != 0 TO ALL;

SELECT 'policy, prefilter: exact search';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'prefilter')
WHERE explain LIKE '%vector_similarity%';

SELECT 'policy, postfilter: uses the index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'postfilter')
WHERE explain LIKE '%vector_similarity%';

-- The planner carries through every table-expression column in the row-policy DAG, including
-- `vec` for the distance sort. Only the predicate dependencies matter: a policy on `id` must
-- retain the non-rescoring rewrite and its virtual `_distance` column.
SELECT 'policy on a non-vector column, no rescoring: uses _distance';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

DROP ROW POLICY rp_04814 ON t_04814;

-- The policy must participate in the runtime `vector_search_index_fetch_multiplier` compensation as well,
-- not only in the `prefilter` bailout. The policy hides the `LIMIT` nearest neighbours (the nearest rows to
-- the reference vector are ids 0, 1, 2, ...), so the index shortlist of `LIMIT` rows is discarded entirely
-- and the query returns nothing; raising the multiplier widens the fetch and the next visible rows are
-- returned. The policy filters on a non-key attribute: a policy on the primary key column feeds the primary
-- key analysis and is carried in `PrewhereInfo`, which makes the rewrite fall back to an exact scan that is
-- correct regardless of the multiplier. Rescoring is disabled explicitly because the rescoring path applies
-- the multiplier regardless of the presence of filters and would mask a regression.
DROP ROW POLICY IF EXISTS rp_04814_mult ON t_04814_mult;
DROP TABLE IF EXISTS t_04814_mult;

CREATE TABLE t_04814_mult (id UInt32, attr UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04814_mult SELECT number, number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

CREATE ROW POLICY rp_04814_mult ON t_04814_mult FOR SELECT USING attr >= 8 TO ALL;

SELECT 'policy hides the nearest neighbours, multiplier 1: too few rows';
SELECT id FROM t_04814_mult ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 4
    SETTINGS vector_search_with_rescoring = 0, vector_search_index_fetch_multiplier = 1.0;

SELECT 'policy hides the nearest neighbours, multiplier 3: the next visible rows';
SELECT id FROM t_04814_mult ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 4
    SETTINGS vector_search_with_rescoring = 0, vector_search_index_fetch_multiplier = 3.0;

DROP ROW POLICY rp_04814_mult ON t_04814_mult;
DROP TABLE t_04814_mult;

-- A row policy that reads the vector column itself. The non-rescoring rewrite removes the physical vector
-- column from the read list, but the policy filter runs inside the reader and needs it, so the rewrite must
-- be skipped for this case (same treatment as a `SELECT` clause containing the vector column).
CREATE ROW POLICY rp_04814 ON t_04814 FOR SELECT USING length(vec) > 0 TO ALL;

SELECT 'policy on the vector column, no rescoring: correct result';
SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'policy on the vector column, rescoring: correct result';
SELECT id FROM t_04814 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

DROP ROW POLICY rp_04814 ON t_04814;
DROP TABLE t_04814;

-- The same, but with FINAL. With `apply_row_policy_after_final = 1` (the default), a policy that is not part of
-- the sorting key is deferred until after the final merge, moving out of `row_level_filter` before the
-- non-rescoring rewrite runs, so the rewrite must inspect the deferred filter as well.
DROP ROW POLICY IF EXISTS rp_04814_final ON t_04814_final;
DROP TABLE IF EXISTS t_04814_final;

CREATE TABLE t_04814_final (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04814_final SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

CREATE ROW POLICY rp_04814_final ON t_04814_final FOR SELECT USING length(vec) > 0 TO ALL;

SELECT 'policy on the vector column, FINAL, no rescoring: correct result';
SELECT id FROM t_04814_final FINAL ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'policy on the vector column, FINAL, rescoring: correct result';
SELECT id FROM t_04814_final FINAL ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

DROP ROW POLICY rp_04814_final ON t_04814_final;
DROP TABLE t_04814_final;
