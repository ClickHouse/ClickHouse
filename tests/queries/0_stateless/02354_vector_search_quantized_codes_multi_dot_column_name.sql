-- The quantized-codes vector search rewrite must never truncate a genuinely multi-dot storage column
-- (`n.values.id`) to a suffix that happens to be another column carrying the `Quantized` codec (`values.id`).
-- Otherwise the shortlist is ranked by the wrong vector column while the query still sorts by the column that was
-- written, and the exact rescore only sees that wrong shortlist, so it returns wrong neighbours instead of leaving
-- the query exact. Same rule as on the vector-similarity-index path (see
-- `02354_vector_search_multi_dot_column_name`).

-- The `Quantized` codec is experimental and gated behind `enable_quantized_codec`.
SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET vector_search_index_fetch_multiplier = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_quantized_multi_dot;

CREATE TABLE t_quantized_multi_dot
(
    id UInt32,
    `values.id` Array(Float32) CODEC(Quantized('int8', 2)),
    `n.values.id` Array(Float32)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- `values.id` grows with `id`, `n.values.id` shrinks with `id`: the nearest neighbours of [0, 1] are the
-- smallest ids by `values.id` and the largest ids by `n.values.id`.
INSERT INTO t_quantized_multi_dot
SELECT number, [toFloat32(number), toFloat32(number + 1)], [toFloat32(64 - number), toFloat32(65 - number)]
FROM numbers(64);

SELECT 'control: ordering by the quantized column uses the shortlist';
SELECT count() > 0 FROM (
    EXPLAIN SELECT id FROM t_quantized_multi_dot ORDER BY L2Distance(`values.id`, [0., 1.]) LIMIT 3)
WHERE explain ILIKE '%quantized shortlist limit%';

SELECT 'control: ordering by the quantized column returns the nearest neighbours';
SELECT id FROM t_quantized_multi_dot ORDER BY L2Distance(`values.id`, [0., 1.]) LIMIT 3;

SELECT 'multi-dot column: no shortlist';
SELECT count() = 0 FROM (
    EXPLAIN SELECT id FROM t_quantized_multi_dot ORDER BY L2Distance(`n.values.id`, [0., 1.]) LIMIT 3)
WHERE explain ILIKE '%quantized shortlist limit%';

SELECT 'multi-dot column: correct result';
SELECT id FROM t_quantized_multi_dot ORDER BY L2Distance(`n.values.id`, [0., 1.]) LIMIT 3;

DROP TABLE t_quantized_multi_dot;
