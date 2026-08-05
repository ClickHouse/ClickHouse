DROP TABLE IF EXISTS my_first_table;

CREATE TABLE my_first_table
(
    user_id UInt32,
    job_id UInt32,
    message String,
    timestamp DateTime,
    metric Float32
)
ENGINE = MergeTree()
PRIMARY KEY (user_id, timestamp);

INSERT INTO my_first_table (user_id, job_id, message, timestamp, metric) VALUES
    (101, 101,'Hello, ClickHouse!',                                 now(),       1   ),
    (101, 102,'Granules are the smallest chunks of data read',      now() + 5,   3 )
    (102, 101,'Insert a lot of rows per batch',                     yesterday(), 2 ),
    (102, 101,'Test1', today(),     1  ),
    (102, 101,'Test2', today(),     2  ),
    (102, 101,'Test3', today(),     2  ),
    (102, 102,'Test4', today(),     4  ),
    (102, 103,'Test5', today(),     4  ),
    (102, 103,'Test6', today(),     1  );

SET enable_analyzer = 1;
SELECT 1 AS constant, user_id, job_id, sum(metric)
FROM my_first_table
GROUP BY constant, user_id, job_id WITH ROLLUP
ORDER BY constant = 0, user_id = 0, job_id = 0, constant, user_id, job_id;

DROP TABLE my_first_table;
