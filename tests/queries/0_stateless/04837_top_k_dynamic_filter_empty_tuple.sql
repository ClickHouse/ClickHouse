DROP TABLE IF EXISTS top_k_empty_tuple;

CREATE TABLE top_k_empty_tuple
(
    value Tuple(),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 64;

INSERT INTO top_k_empty_tuple SELECT tuple(), number FROM numbers(100000);

SELECT value
FROM top_k_empty_tuple
ORDER BY ALL DESC
LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1;

DROP TABLE top_k_empty_tuple;
