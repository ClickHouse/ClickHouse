-- https://github.com/ClickHouse/ClickHouse/issues/57243
-- https://github.com/ClickHouse/ClickHouse/issues/93343

SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;

SELECT count()
FROM
(
    SELECT range(300)::Array(LowCardinality(UInt64)) AS arr
    FROM numbers(3)
    ARRAY JOIN arr
    SETTINGS enable_unaligned_array_join = 1
);

SELECT count()
FROM
(
    SELECT
        range(300)::Array(LowCardinality(UInt64)) AS arr1,
        range(257)::Array(LowCardinality(UInt64)) AS arr2
    FROM numbers(3)
    ARRAY JOIN arr1, arr2
    SETTINGS enable_unaligned_array_join = 1
);

SELECT count()
FROM
(
    SELECT range(257)::Array(LowCardinality(UInt16)) AS arr
    FROM numbers(3)
    ARRAY JOIN arr
    SETTINGS enable_unaligned_array_join = 1
);
