-- set max_block_size = 30000;
DROP TABLE IF EXISTS test_table;
-- DROP TABLE test_table_100k;

CREATE TABLE test_table (
    id UInt64,
    data String
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_table
SELECT number AS id, concat('data_', toString(number)) AS data
FROM numbers(4);

-- CREATE TABLE test_table_100k (
--     id UInt64,
--     data String
-- ) ENGINE = MergeTree()
-- ORDER BY id;

-- INSERT INTO test_table_100k
-- SELECT number AS id, concat('data_', toString(number)) AS data
-- FROM numbers(100);

-- Only FROM expression is provided:
SELECT '[FROM] Query 1 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM id = 0;

SELECT '[FROM] Query 2 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM id = 3;

SELECT '[FROM] Query 3 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM data LIKE 'data_2';

SELECT '[FROM] Query 4 result:';

SELECT * FROM test_table
LIMIT INRANGE FROM id = -1; 


SELECT '--------------------';


-- Only TO expression is provided:
SELECT '[TO] Query 1 result:';

SELECT * FROM test_table
LIMIT INRANGE TO id = 0;

SELECT '[TO] Query 2 result:';

SELECT * FROM test_table
LIMIT INRANGE TO id = 3;

SELECT '[TO] Query 3 result:';

SELECT * FROM test_table
LIMIT INRANGE TO data LIKE 'data_2';

-- SELECT '[TO] Query 4 result:';

-- SELECT * FROM test_table
-- LIMIT INRANGE TO id = -1; -- all data returned, exception thrown.


-- Chunk tests:
-- SELECT * FROM test_table_100k LIMIT INRANGE FROM id = 60000;
-- SELECT * FROM test_table_100k LIMIT INRANGE TO id = 60000;
