-- set max_block_size = 30000;
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    user_id UInt32,
    message String,
    metric Float32
)
ENGINE = MergeTree
PRIMARY KEY (user_id);

INSERT INTO test_table (user_id, message, metric) VALUES
    (101, 'Hello, ClickHouse!',                                     -1.0    ),
    (101, 'Granules are the smallest chunks of data read',          3.14159 ),
    (102, 'Insert a lot of rows per batch',                         1.41421 ),
    (102, 'Sort your data based on your commonly-used queries',     2.718   );


-- FROM and TO expressions are provided:
SELECT '[FROM, TO] Query 1 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM message LIKE 'Hello, ClickHouse!' TO message LIKE 'Insert a lot of rows per batch';

SELECT '[FROM, TO] Query 2 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM metric >= -1 TO metric > 1 AND metric < 3;

SELECT '[FROM, TO] Query 3 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM message = 'Granules are the smallest chunks of data read' TO metric > 1; -- different columns.

SELECT '[FROM, TO] Query 4 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM metric = 123123 TO metric < 3; -- FROM index is not found, nothing returned.

-- SELECT '[FROM, TO] Query 5 result:';

-- SELECT * FROM test_table
-- LIMIT INRANGE FROM metric > -2 TO metric = 123123; -- If the TO index is not found [exception is thrown], returns data from the FROM index to the end of the table.

-- Chunk tests:
