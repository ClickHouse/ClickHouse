-- Tags: no-random-settings, no-parallel-replicas
set enable_analyzer=1;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    `x` String,
    INDEX idx1 x TYPE bloom_filter GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity=1;
INSERT INTO test SELECT number FROM numbers(1000);
EXPLAIN indexes = 1 SELECT * FROM test WHERE CAST(x, 'String') = '100';
DROP TABLE test;

CREATE TABLE test
(
    `x` String,
    INDEX idx1 x TYPE set(0) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity=1;
INSERT INTO test SELECT number FROM numbers(1000);
EXPLAIN indexes = 1 SELECT * FROM test WHERE CAST(x, 'String') = '100';
DROP TABLE test;

CREATE TABLE test
(
    `x` String,
    INDEX idx1 x TYPE tokenbf_v1(16000, 2, 0) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity=1;
INSERT INTO test SELECT number FROM numbers(1000);
EXPLAIN indexes = 1 SELECT * FROM test WHERE CAST(x, 'String') = '100';
DROP TABLE test;

CREATE TABLE test
(
    `x` String,
    INDEX idx1 x TYPE ngrambf_v1(4, 16000, 2, 0)  GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity=1;
INSERT INTO test SELECT number FROM numbers(1000);
EXPLAIN indexes = 1 SELECT * FROM test WHERE CAST(x, 'String') = '100';
DROP TABLE test;

CREATE TABLE test
(
    `x` String,
    INDEX idx1 x TYPE minmax GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity=1;
INSERT INTO test SELECT number FROM numbers(1000);
EXPLAIN indexes = 1 SELECT * FROM test WHERE CAST(x, 'String') = '100';
DROP TABLE test;

