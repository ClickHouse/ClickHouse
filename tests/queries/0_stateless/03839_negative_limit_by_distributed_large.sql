-- Tags: distributed

-- These queries exercise multi-stream distributed processing with negative LIMIT BY.
-- If pipeline scheduling or port handling is incorrect, they will most likely fail with `Pipeline stuck`
-- instead of completing successfully.
DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test SELECT number FROM numbers(200000);

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id, id % 7 AS g
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY g, id
    LIMIT -2147483648 BY g
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id, id % 7 AS g
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY g, id
    LIMIT -100 OFFSET -2 BY g
)
ORDER BY ALL
FORMAT Null;
