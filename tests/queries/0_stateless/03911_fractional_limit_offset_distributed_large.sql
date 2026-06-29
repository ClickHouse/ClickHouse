-- Tags: distributed

-- These queries almost exhaustively exercise multiple parts of multi-stream distributed processing.
-- If pipeline scheduling or port handling is incorrect, they will most likely fail with `Pipeline stuck`
-- instead of completing successfully.
DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test SELECT number FROM numbers(200000);

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    LIMIT 0.2
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    LIMIT 0.2, 0.7
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    LIMIT 100000, 0.7
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY id
    LIMIT 0.2 WITH TIES
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    ORDER BY id
    LIMIT 0.5, 0.2 WITH TIES
)
ORDER BY ALL
FORMAT Null;

SELECT
    concat(current_database(), '')
FROM
(
    SELECT id
    FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test)
    OFFSET 0.5
)
ORDER BY ALL
FORMAT Null;
