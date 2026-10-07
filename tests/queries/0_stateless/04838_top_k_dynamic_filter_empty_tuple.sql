SET enable_nullable_tuple_type = 1;
SET max_threads = 1;
SET max_block_size = 8192;
SET query_plan_max_limit_for_top_k_optimization = 100;
SET use_skip_indexes_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;

DROP TABLE IF EXISTS top_k_empty_tuple;

CREATE TABLE top_k_empty_tuple
(
    direct Tuple(),
    normal Tuple(UInt64),
    nested Tuple(Tuple()),
    nullable_nested Tuple(Nullable(Tuple())),
    mixed Tuple(UInt64, Tuple()),
    array_nested Array(Tuple()),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 64;

INSERT INTO top_k_empty_tuple
SELECT
    tuple(),
    tuple(number),
    tuple(tuple()),
    tuple(CAST(if(number >= 99996, NULL, tuple()), 'Nullable(Tuple())')),
    tuple(number, tuple()),
    if(number >= 99996, [tuple(), tuple()], if(number = 99995, [tuple()], [])),
    number
FROM numbers(100000);

SELECT 'direct_has_filter', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT direct FROM top_k_empty_tuple ORDER BY direct DESC LIMIT 5
)
WHERE explain LIKE '%__topKFilter%';

SELECT 'normal_has_filter', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT normal FROM top_k_empty_tuple ORDER BY normal DESC LIMIT 5
)
WHERE explain LIKE '%__topKFilter%';

SELECT 'nested_has_filter', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT nested FROM top_k_empty_tuple ORDER BY nested DESC LIMIT 5
)
WHERE explain LIKE '%__topKFilter%';

SELECT 'nullable_nested_has_filter', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT nullable_nested FROM top_k_empty_tuple ORDER BY nullable_nested DESC NULLS FIRST LIMIT 5
)
WHERE explain LIKE '%__topKFilter%';

SELECT 'mixed_has_filter', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT mixed FROM top_k_empty_tuple ORDER BY mixed DESC LIMIT 5
)
WHERE explain LIKE '%__topKFilter%';

SELECT 'array_nested_has_filter', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT array_nested FROM top_k_empty_tuple ORDER BY array_nested DESC LIMIT 5
)
WHERE explain LIKE '%__topKFilter%';

SELECT 'direct_results';
SELECT direct FROM top_k_empty_tuple ORDER BY ALL DESC LIMIT 5;

SELECT 'normal_results';
SELECT normal FROM top_k_empty_tuple ORDER BY ALL DESC LIMIT 5;

SELECT 'nested_results';
SELECT nested FROM top_k_empty_tuple ORDER BY ALL DESC LIMIT 5;

SELECT 'nullable_nested_results';
SELECT nullable_nested FROM top_k_empty_tuple ORDER BY nullable_nested DESC NULLS FIRST LIMIT 5;

SELECT 'mixed_results';
SELECT mixed FROM top_k_empty_tuple ORDER BY ALL DESC LIMIT 5;

SELECT 'array_nested_results';
SELECT array_nested FROM top_k_empty_tuple ORDER BY ALL DESC LIMIT 5;

DROP TABLE top_k_empty_tuple;
