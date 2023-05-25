-- Tags: no-fasttest
CREATE TABLE IF NOT EXISTS aboba
(
    `user_id` UInt32,
    `message` String,
    `timestamp` DateTime,
    `metric` Float32
)
ENGINE = MergeTree
ORDER BY user_id;
INSERT INTO aboba (user_id, message, timestamp, metric) VALUES (101, 'Hello, ClickHouse!', now(), -1.0), (102, 'Insert a lot of rows per batch', yesterday(), 1.41421 ), (102, 'Sort your data based on your commonly-used queries', today(), 2.718), (101, 'Granules are the smallest chunks of data read', now() + 5, 3.14159);
SET dialect = 'prql';
from aboba
select user_id
