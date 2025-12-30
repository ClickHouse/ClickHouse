-- https://github.com/ClickHouse/ClickHouse/issues/54511

DROP TABLE IF EXISTS my_first_table;

CREATE TABLE my_first_table
(
    user_id UInt32,
    message String,
    timestamp DateTime,
    metric Float32
)
ENGINE = MergeTree
PRIMARY KEY (user_id, timestamp);

INSERT INTO my_first_table (user_id, message, timestamp, metric) VALUES
    (101, 'Hello, ClickHouse!',                                 now(),       -1.0    ),     (102, 'Insert a lot of rows per batch',                     yesterday(), 1.41421 ),    (102, 'Sort your data based on your commonly-used queries', today(),     2.718   ),    (101, 'Granules are the smallest chunks of data read',      now() + 5,   3.14159 );

SET enable_analyzer=1;

SELECT
    user_id
     , (count(user_id) OVER (PARTITION BY user_id)) AS count
FROM my_first_table
WHERE timestamp > 0 and user_id IN (101)
LIMIT 2 BY user_id;

DROP TABLE IF EXISTS my_first_table;
