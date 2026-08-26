-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- A search column whose real storage name contains a dot (e.g. a `Nested` component `n.vec`) must be
-- resolved as-is. Stripping everything before the first dot unconditionally would turn `n.vec` into
-- `vec` and bind the query to the index of an unrelated column, producing wrong results.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_05043;

CREATE TABLE t_05043 (id UInt32, vec Array(Float32), `n.vec` Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2),
    INDEX idx_n_vec `n.vec` TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- `vec` grows along the first dimension, `n.vec` along the second one, so the nearest neighbour of
-- [0, 63] differs between the two columns: `n.vec` gives id 63, `vec` would give id 0.
INSERT INTO t_05043 SELECT number, [toFloat32(number), 0.], [0., toFloat32(number)] FROM numbers(64);

SELECT 'nearest neighbour of the dotted search column, with rescoring';
SELECT id FROM t_05043 ORDER BY L2Distance(`n.vec`, [0., 63.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 1;

SELECT 'nearest neighbour of the dotted search column, without rescoring';
SELECT id FROM t_05043 ORDER BY L2Distance(`n.vec`, [0., 63.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

SELECT 'the same as the exact scan';
SELECT id FROM t_05043 ORDER BY L2Distance(`n.vec`, [0., 63.]) LIMIT 1
    SETTINGS use_skip_indexes = 0;

SELECT 'the dotted search column is replaced by `_distance`';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id FROM t_05043 ORDER BY L2Distance(`n.vec`, [0., 63.]) LIMIT 1
        SETTINGS vector_search_with_rescoring = 0)
WHERE explain LIKE '%_distance%';

SELECT 'the index of the dotted search column is used';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_05043 ORDER BY L2Distance(`n.vec`, [0., 63.]) LIMIT 1)
WHERE explain LIKE '%Name: idx%';

-- The plain column must keep working next to the dotted one.
SELECT 'nearest neighbour of the plain search column';
SELECT id FROM t_05043 ORDER BY L2Distance(vec, [0., 63.]) LIMIT 1
    SETTINGS vector_search_with_rescoring = 0;

DROP TABLE t_05043;
