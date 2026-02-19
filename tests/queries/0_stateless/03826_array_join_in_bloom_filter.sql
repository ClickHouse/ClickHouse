-- https://github.com/ClickHouse/ClickHouse/issues/21558

DROP TABLE IF EXISTS test_array_bloom;

CREATE TABLE test_array_bloom (
    id UInt16,
    ts DateTime,
    data Array(String),
    INDEX test_bloom data TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY id;

INSERT INTO test_array_bloom VALUES (1, '2021-01-01', ['aaa', 'bbb']);
INSERT INTO test_array_bloom VALUES (2, '2021-01-01', ['ccc']);

SELECT id FROM test_array_bloom WHERE has(data, 'ccc');
SELECT id FROM test_array_bloom ARRAY JOIN data WHERE data IN ('aaa') ORDER BY id;

DROP TABLE test_array_bloom;
