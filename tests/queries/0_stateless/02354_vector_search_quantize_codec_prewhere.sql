-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertion below
--  cannot hold there; the query still returns exact results in that case.)
-- Regression test: the quantized-vector-search rewrite pulls the `<column>.quantized` companion subcolumn into the read
-- after query analysis. When the query also has a PREWHERE, the added subcolumn must be passed through the PREWHERE
-- ActionsDAG (which otherwise only outputs the columns it was built with), otherwise it would be dropped after PREWHERE
-- and the shortlist would have no codes to rank. This exercises `ReadFromMergeTree::addReadColumn`.

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS quantize_prewhere;
CREATE TABLE quantize_prewhere
(
    id UInt32,
    tag UInt8,
    vec Array(Float32) CODEC(Quantized('int8', 8))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_prewhere
SELECT number, number % 3, arrayMap(j -> toFloat32(sipHash64(number, j) % 100), range(8))
FROM numbers(500);

-- The rewrite still engages when a PREWHERE is present: the shortlist ranks with the codes read alongside PREWHERE.
SELECT 'plan_has_shortlist_under_prewhere',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_prewhere PREWHERE tag = 1
    ORDER BY L2Distance(vec, (SELECT vec FROM quantize_prewhere WHERE id = 42)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- With a shortlist covering all matching rows, the codes path reproduces the exact brute-force top-k under PREWHERE.
WITH (SELECT vec FROM quantize_prewhere WHERE id = 42) AS ref
SELECT 'prewhere_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_prewhere PREWHERE tag = 1 ORDER BY d, id LIMIT 5))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_prewhere PREWHERE tag = 1 ORDER BY L2Distance(vec, ref) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000));

DROP TABLE quantize_prewhere;
