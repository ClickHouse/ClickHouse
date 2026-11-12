CREATE TABLE test_table
(
    `key` UInt32,
    `_part_offset` DEFAULT 0
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test_table (key) SELECT number
FROM numbers(10);

SELECT *
FROM test_table;

SELECT
    key,
    _part_offset
FROM test_table;
