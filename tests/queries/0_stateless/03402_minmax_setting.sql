
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` int,
    `b` int
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE test (ADD INDEX idx b TYPE minmax GRANULARITY 1);

ALTER TABLE test MODIFY COLUMN b MODIFY SETTING min_compress_block_size = 8192;