-- Multi-key GROUP BY with LowCardinality keys uses serialized aggregation, whose keys must not
-- depend on the dictionary order of the individual parts: the keys are serialized by value.

DROP TABLE IF EXISTS lc_serialized_group_by;
CREATE TABLE lc_serialized_group_by
(
    s LowCardinality(String),
    f LowCardinality(FixedString(4)),
    n LowCardinality(Nullable(String)),
    p Nullable(String),
    k UInt64,
    v UInt64
)
ENGINE = MergeTree ORDER BY k;

-- The second part builds its LowCardinality dictionaries in a different order.
INSERT INTO lc_serialized_group_by SELECT toString(number % 7), toFixedString(toString(number % 5), 4), if(number % 11 = 0, NULL, toString(number % 3)), if(number % 7 = 0, NULL, toString(number % 4)), number, number FROM numbers(3000);
INSERT INTO lc_serialized_group_by SELECT toString((number * 3 + 2) % 7), toFixedString(toString((number * 4 + 1) % 5), 4), if(number % 5 = 0, NULL, toString((number * 2 + 1) % 3)), if(number % 3 = 0, NULL, toString((number + 1) % 5)), number, number FROM numbers(3000);

SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f;
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f SETTINGS max_threads = 1, group_by_two_level_threshold = 1;
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f SETTINGS max_block_size = 64;
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f LIMIT 5;
SELECT s, k % 4 AS m, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, m ORDER BY s, m;
SELECT s, n, count() FROM lc_serialized_group_by GROUP BY s, n ORDER BY s, n NULLS FIRST;
SELECT s, p, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, p ORDER BY s, p NULLS FIRST;
SELECT s, p, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, p ORDER BY s, p NULLS FIRST SETTINGS max_block_size = 64, group_by_two_level_threshold = 1;

DROP TABLE IF EXISTS lc_serialized_group_by;

-- A top-K heap can freeze and fall back to ordinary aggregation in the middle of a query. The
-- worst `(s, f)` pair in the sort order is rare and the deliberately small observation window makes
-- the heap freeze after the first block, so the remaining blocks are aggregated without the ranked
-- columns.
DROP TABLE IF EXISTS lc_serialized_group_by_topk;
CREATE TABLE lc_serialized_group_by_topk
(
    s LowCardinality(String),
    f LowCardinality(FixedString(4)),
    v UInt64
)
ENGINE = MergeTree ORDER BY v;

INSERT INTO lc_serialized_group_by_topk SELECT if(number % 1000 < 995, 'a', 'b'), if(number % 1000 < 500, toFixedString('x', 4), toFixedString('y', 4)), number FROM numbers(200000);

SELECT s, f, count(), sum(v) FROM lc_serialized_group_by_topk GROUP BY s, f ORDER BY s, f LIMIT 2 SETTINGS max_threads = 1, max_block_size = 8192, group_by_top_k_optimization_observation_rows = 1;

DROP TABLE IF EXISTS lc_serialized_group_by_topk;
