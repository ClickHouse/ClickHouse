-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- An unrelated column whose dotted name ends with the name of the search column (`n.vec` vs `vec`)
-- must not be mistaken for the search column: queries reading `n.vec` in the projection, in `WHERE`,
-- or in a row policy keep the optimized `_distance` rewrite of the vector-search
-- `ORDER BY <distance> LIMIT` plan.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_05042;

CREATE TABLE t_05042 (id UInt32, vec Array(Float32), `n.vec` Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_05042 SELECT number, [toFloat32(number), toFloat32(number + 1)], [toFloat32(number)] FROM numbers(64);

SELECT 'no reference to the dotted column (control): the vector column is replaced';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_05042 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

SELECT 'projection on the unrelated dotted column: correct result';
SELECT id, length(`n.vec`) FROM t_05042 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'projection on the unrelated dotted column: the vector column is replaced';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, length(`n.vec`) FROM t_05042 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

SELECT 'WHERE on the unrelated dotted column: correct result';
SELECT id FROM t_05042 WHERE length(`n.vec`) = 1 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'WHERE on the unrelated dotted column: the vector column is replaced';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_05042 WHERE length(`n.vec`) = 1 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

-- Reading the search column itself must still disable the rewrite.
SELECT 'projection on the search column: the vector column is kept';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT id, length(vec) FROM t_05042 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

DROP ROW POLICY IF EXISTS policy_05042 ON t_05042;
CREATE ROW POLICY policy_05042 ON t_05042 USING length(`n.vec`) = 1 TO CURRENT_USER;

SELECT 'row policy on the unrelated dotted column: correct result';
SELECT id FROM t_05042 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'row policy on the unrelated dotted column: the vector column is replaced';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_05042 ORDER BY cosineDistance(vec, [0., 1.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

DROP ROW POLICY policy_05042 ON t_05042;
DROP TABLE t_05042;
