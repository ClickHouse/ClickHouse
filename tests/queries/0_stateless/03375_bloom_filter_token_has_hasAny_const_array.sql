SET parallel_replicas_local_plan=1;

-- Force using skip indexes in planning to proper test with EXPLAIN indexes = 1.
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS bloom_filter_has_const_array;

CREATE TABLE bloom_filter_has_const_array
(
    `bf` String,
    `abf` Array(String),
    INDEX idx_bf bf TYPE tokenbf_v1(512,3,0) GRANULARITY 1,
    INDEX idx_abf abf TYPE tokenbf_v1(512,3,0) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY ()
SETTINGS index_granularity=1;

INSERT INTO bloom_filter_has_const_array
VALUES ('a', ['a','a']), ('b', ['b','b']), ('c', ['c','c']), ('d',['d','e']);

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT bf
    FROM bloom_filter_has_const_array
    WHERE hasAny(['a','c','d'], abf)
)
WHERE explain LIKE 'Description%' or explain LIKE 'Granules%';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT bf
    FROM bloom_filter_has_const_array
    WHERE has(['a','d'], bf)
)
WHERE explain LIKE 'Description%' or explain LIKE 'Granules%';

SELECT bf
FROM bloom_filter_has_const_array
WHERE hasAny(['a','c','d'], abf) and has(['a','d'], bf) and hasAll(['d','e'], abf);

DROP TABLE IF EXISTS bloom_filter_has_const_array;

