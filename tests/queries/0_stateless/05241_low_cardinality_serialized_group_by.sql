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
