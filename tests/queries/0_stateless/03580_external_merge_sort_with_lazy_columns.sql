DROP TABLE IF EXISTS test SYNC;
CREATE TABLE test
(
    c1 LowCardinality(String),
    c2 LowCardinality(String),
    c3 UInt32
)
ENGINE = MergeTree
ORDER BY (c1, c2, c3)
SETTINGS index_granularity = 8192;

INSERT INTO test SELECT toString(number) AS c1, c1 AS c2, number AS c3 FROM system.numbers LIMIT 2;

-- weird settings to force external sort
SELECT * FROM test ORDER BY c3 LIMIT 1 SETTINGS max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0.0;
