-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Regression test for the vector-search `ORDER BY <distance> LIMIT` rewrite when the projection
-- reads the vector column. After removing the distance-sort output, the projection still needs
-- `vec` as an input; the rewrite must keep the physical column rather than replacing it with
-- `_distance` and failing with `NOT_FOUND_COLUMN_IN_BLOCK`.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_04926;

CREATE TABLE t_04926 (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_04926 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64);

SELECT 'SELECT on the vector column, no rescoring: correct result';
SELECT length(vec) FROM t_04926 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

-- The control proves the non-rescoring rewrite can happen; the projection query must disable it.
SELECT 'no vector projection, no rescoring: the vector column is replaced (control)';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_04926 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

SELECT 'SELECT on the vector column, no rescoring: the vector column is kept';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT length(vec) FROM t_04926 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

-- The plain `SELECT vec` form produces a different projection DAG: the distance function reads the
-- `INPUT` node directly, so the search column name extracted from `ORDER BY` loses its table
-- qualifier while the DAG nodes keep it (`__table1.vec`). Both forms must be recognized.
SELECT 'plain SELECT of the vector column, no rescoring: correct result';
SELECT id, vec FROM t_04926 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'plain SELECT of the vector column, no rescoring: the vector column is kept';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT id, vec FROM t_04926 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

DROP TABLE t_04926;
