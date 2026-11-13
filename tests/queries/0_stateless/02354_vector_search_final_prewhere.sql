-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Regression test for the vector-search `ORDER BY <distance> LIMIT` rewrite under `FINAL` with an explicit
-- `PREWHERE` that reads the vector column.
--
-- An explicit `PREWHERE` turns the non-rescoring rewrite off, because that rewrite drops the physical vector
-- column from the read list and replaces it with the virtual `_distance` column, which the `PREWHERE` filter
-- inside the reader could not evaluate. Under `FINAL` the `PREWHERE` can additionally be deferred until after
-- the final merge - either through `apply_prewhere_after_final`, or because a row policy over a non-sorting-key
-- column is deferred and `PREWHERE` must stay below the policy. Deferring copies the filter into
-- `deferred_prewhere_info` and keeps `query_info.prewhere_info` in place until the pipeline is built, i.e. past
-- every query plan optimization, so the bailout still fires and the vector column survives for the deferred
-- filter.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
-- The control query below relies on the non-rescoring rewrite actually happening under `FINAL`, which requires
-- the vector index to be used. Pin the settings against randomization.
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 0;

DROP ROW POLICY IF EXISTS rp_04839 ON t_04839;
DROP TABLE IF EXISTS t_04839;

CREATE TABLE t_04839 (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04839 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

SELECT 'FINAL, PREWHERE on the vector column, no rescoring: correct result';
SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'FINAL, PREWHERE on the vector column, rescoring: correct result';
SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

-- The same, with the `PREWHERE` explicitly deferred until after the final merge.

SELECT 'FINAL, deferred PREWHERE, no rescoring: correct result';
SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0, apply_prewhere_after_final = 1;

SELECT 'FINAL, deferred PREWHERE, rescoring: correct result';
SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1, apply_prewhere_after_final = 1;

-- The vector column must still be read: the rewrite to `_distance` must not happen. The control below runs the
-- same query without the `PREWHERE`, where the rewrite does happen, so that the check cannot pass vacuously.
SELECT 'FINAL, no PREWHERE, no rescoring: the vector column is replaced (control)';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04839 FINAL ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

SELECT 'FINAL, deferred PREWHERE, no rescoring: the vector column is kept';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0, apply_prewhere_after_final = 1)
WHERE explain LIKE '%_distance%';

-- The same, but the deferral is caused by a row policy over a non-sorting-key column: the policy is applied
-- after `FINAL` and `PREWHERE` follows it.

CREATE ROW POLICY rp_04839 ON t_04839 FOR SELECT USING length(vec) > 1 TO ALL;

SELECT 'FINAL, PREWHERE deferred behind a row policy, no rescoring: correct result';
SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'FINAL, PREWHERE deferred behind a row policy, rescoring: correct result';
SELECT id FROM t_04839 FINAL PREWHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

DROP ROW POLICY rp_04839 ON t_04839;
DROP TABLE t_04839;
