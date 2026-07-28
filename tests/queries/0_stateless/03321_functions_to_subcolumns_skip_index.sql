-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS bloom_filter_test;

CREATE TABLE bloom_filter_test
(
    id UInt64,
    m Map(String, String),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO bloom_filter_test VALUES (1, {'1': '1'}), (2, {'2': '2'}), (3, {'3': '3'});

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT id          -- 'm' not in projection columns
    FROM bloom_filter_test
    WHERE mapContains(m, '1')
    ORDER BY id
) WHERE explain LIKE '%Granules:%';

SELECT id          -- 'm' not in projection columns
FROM bloom_filter_test
WHERE mapContains(m, '1')
ORDER BY id;

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT *           -- 'm' in projection columns
    FROM bloom_filter_test
    WHERE mapContains(m, '1')
    ORDER BY id
) WHERE explain LIKE '%Granules:%';

SELECT *           -- 'm' in projection columns
FROM bloom_filter_test
WHERE mapContains(m, '1')
ORDER BY id;

DROP TABLE bloom_filter_test;
