-- Tags: distributed

-- These queries exercise multi-stream distributed processing with LIMIT AFTER/UNTIL.
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
    ORDER BY id
    LIMIT 100 AFTER id >= 100000
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
    LIMIT 1000 AFTER id >= 50000 UNTIL id >= 150000
)
ORDER BY ALL
FORMAT Null;

-- { echo }

-- LIMIT 3 AFTER (distributed, both analyzers)
SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 3 AFTER id >= 199995;
SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 3 AFTER id >= 199995 SETTINGS enable_analyzer = 0;

-- LIMIT AFTER UNTIL (distributed, both analyzers)
SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 100 AFTER id >= 199990 UNTIL id >= 199995;
SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 100 AFTER id >= 199990 UNTIL id >= 199995 SETTINGS enable_analyzer = 0;

-- LIMIT AFTER ALL (distributed, both analyzers)
SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 2 AFTER id IN (199990, 199995) ALL;
SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 2 AFTER id IN (199990, 199995) ALL SETTINGS enable_analyzer = 0;

DROP TABLE test;
