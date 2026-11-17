-- https://github.com/ClickHouse/ClickHouse/issues/93343
-- https://github.com/ClickHouse/ClickHouse/issues/57243

SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;

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

-- Original reproducer from #57243: ARRAY JOIN of LowCardinality array and a Map of differing lengths.
DROP TABLE IF EXISTS arrays_test_57243;

CREATE TABLE arrays_test_57243
(
    `s` String,
    `arr1` Array(LowCardinality(UInt8)),
    `map1` Map(UInt8, String),
    `map2` Map(UInt8, String)
)
ENGINE = Log();

INSERT INTO arrays_test_57243 VALUES
    ('Hello', [1, 2], map(1, '1', 2, '2'), map(1, '1')),
    ('World', [3, 4, 5], map(3, '3', 4, '4', 5, '5'), map(3, '3', 4, '4')),
    ('Goodbye', [], map(), map());

SELECT count()
FROM
(
    SELECT s, arr1, map1
    FROM
    (
        SELECT s, range(0, 1000)::Array(LowCardinality(UInt64)) AS arr1, map1, map2
        FROM arrays_test_57243
    )
    ARRAY JOIN arr1, map1
    SETTINGS enable_unaligned_array_join = 1
);

DROP TABLE arrays_test_57243;
