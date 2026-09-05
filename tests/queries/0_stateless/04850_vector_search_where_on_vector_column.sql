-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Regression test for the vector-search `ORDER BY <distance> LIMIT` rewrite with a plain `WHERE` filter that
-- reads the vector column. The non-rescoring rewrite drops the physical vector column from the read list and
-- rebuilds the `FilterStep` on top of the new reader header, so a predicate that reads the vector column would
-- be left without its input and the query would fail with `NOT_FOUND_COLUMN_IN_BLOCK`. The rewrite must be
-- skipped instead, the same way it is skipped when the `SELECT` clause or a row policy reads the vector column.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_04850;

CREATE TABLE t_04850 (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04850 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

SELECT 'WHERE on the vector column, no rescoring, no implicit PREWHERE: correct result';
SELECT id FROM t_04850 WHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_prewhere = 0;

SELECT 'WHERE on the vector column, no rescoring, default PREWHERE settings: correct result';
SELECT id FROM t_04850 WHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'WHERE on the vector column, rescoring: correct result';
SELECT id FROM t_04850 WHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

-- The vector column must still be read: the rewrite to `_distance` must not happen. The control below runs the
-- same query without the `WHERE`, where the rewrite does happen, so that the check cannot pass vacuously.
SELECT 'no WHERE, no rescoring: the vector column is replaced (control)';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04850 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

SELECT 'WHERE on the vector column, no rescoring: the vector column is kept';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04850 WHERE length(vec) > 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_prewhere = 0)
WHERE explain LIKE '%_distance%';

-- A `WHERE` filter over other columns must not turn the rewrite off.
SELECT 'WHERE on another column, no rescoring: the vector column is replaced';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04850 WHERE id >= 0 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0, query_plan_optimize_prewhere = 0)
WHERE explain LIKE '%_distance%';

DROP TABLE t_04850;
