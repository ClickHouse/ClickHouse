-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas

DROP TABLE IF EXISTS index_test;
CREATE TABLE index_test
(
    id UInt32,
    arr Array(String),
    INDEX array_index arrayMap(x -> lower(x), arr) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX array_index_2 arrayMap((x, y) -> concat(lower(x), y), arr, arr) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX array_index_3 arrayMap((x, y) -> concat(lower(x), y, '_', toString(id)), arr, arr) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_suspicious_indices = 1, index_granularity = 4;

insert into index_test select number, arrayMap(x -> 'A_' || toString(x) , range(number)) from numbers(16);

-- { echo On }

EXPLAIN indexes = 1, description=0
SELECT arr
FROM index_test
WHERE has(arrayMap(x -> lower(x), arr), lower('a_12'))
SETTINGS enable_analyzer = 1;

SELECT arr
FROM index_test
WHERE has(arrayMap(x -> lower(x), arr), lower('a_12'))
SETTINGS enable_analyzer = 1;


EXPLAIN indexes = 1, description=0
SELECT arr
FROM index_test
WHERE has(arrayMap((x, y) -> concat(lower(x), y), arr, arr), 'a_12A_12')
SETTINGS enable_analyzer = 1;

SELECT arr
FROM index_test
WHERE has(arrayMap((x, y) -> concat(lower(x), y), arr, arr), 'a_12A_12')
SETTINGS enable_analyzer = 1;

EXPLAIN indexes = 1, description=0
SELECT arr
FROM index_test
WHERE has(arrayMap((x, y) -> concat(lower(x), y, '_', toString(id)), arr, arr), 'a_12A_12_13')
SETTINGS enable_analyzer = 1;

SELECT arr
FROM index_test
WHERE has(arrayMap((x, y) -> concat(lower(x), y, '_', toString(id)), arr, arr), 'a_12A_12_13')
SETTINGS enable_analyzer = 1;
