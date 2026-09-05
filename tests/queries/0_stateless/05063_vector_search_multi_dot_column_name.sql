-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- A genuinely multi-dot storage column (`n.values.id`) must never be truncated to a suffix that happens to
-- be another indexed column (`values.id`). Otherwise the vector search optimization ranks candidates by the
-- wrong vector column while the query still sorts by the column that was written, which returns wrong
-- neighbours instead of falling back to an exact scan.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_05063;

CREATE TABLE t_05063 (
    id UInt32,
    `values.id` Array(Float32),
    `n.values.id` Array(Float32),
    INDEX idx `values.id` TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- `values.id` grows with `id`, `n.values.id` shrinks with `id`: the nearest neighbours of [0, 1] are the
-- smallest ids by `values.id` and the largest ids by `n.values.id`.
INSERT INTO t_05063
SELECT number, [toFloat32(number), toFloat32(number + 1)], [toFloat32(64 - number), toFloat32(65 - number)]
FROM numbers(64);

SELECT 'control: ordering by the indexed column uses the index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_05063 ORDER BY L2Distance(`values.id`, [0., 1.]) LIMIT 3)
WHERE explain LIKE '%idx%';

SELECT 'control: ordering by the indexed column returns the nearest neighbours';
SELECT id FROM t_05063 ORDER BY L2Distance(`values.id`, [0., 1.]) LIMIT 3;

SET enable_analyzer = 1;

SELECT 'multi-dot column, analyzer: the index of the suffix column is not used';
SELECT count() = 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_05063 ORDER BY L2Distance(`n.values.id`, [0., 1.]) LIMIT 3)
WHERE explain LIKE '%idx%';

SELECT 'multi-dot column, analyzer: correct result';
SELECT id FROM t_05063 ORDER BY L2Distance(`n.values.id`, [0., 1.]) LIMIT 3;

SET enable_analyzer = 0;

SELECT 'multi-dot column, old analyzer: the index of the suffix column is not used';
SELECT count() = 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_05063 ORDER BY L2Distance(`n.values.id`, [0., 1.]) LIMIT 3)
WHERE explain LIKE '%idx%';

SELECT 'multi-dot column, old analyzer: correct result';
SELECT id FROM t_05063 ORDER BY L2Distance(`n.values.id`, [0., 1.]) LIMIT 3;

SET enable_analyzer = 1;

DROP TABLE t_05063;
