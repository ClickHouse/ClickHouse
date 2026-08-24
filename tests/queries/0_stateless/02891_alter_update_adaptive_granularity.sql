CREATE TABLE kv
(
    `key` UInt64,
    `value` UInt64,
    `s` String,
    INDEX value_idx value TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY key
SETTINGS index_granularity = 32, index_granularity_bytes = 1024;

INSERT INTO kv SELECT
    number,
    number + 100,
    toString(number)
FROM numbers(2048);

ALTER TABLE kv
    UPDATE s = 'The Containers library is a generic collection of class templates and algorithms that allow programmers to easily implement common data structures like queues, lists and stacks' WHERE 1
SETTINGS mutations_sync = 2;

SELECT *
FROM kv
WHERE value = 442;
