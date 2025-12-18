-- Tests how squashing combines blocks from INSERT...SELECT based on min/max thresholds
-- 1. Creates 4 test tables
-- 2. Inserts 100 rows (10 blocks of 10 rows each from SELECT) with different squashing thresholds:
--    - Test 1: max_insert_block_size_rows=50 (squash until 50 rows, then emit)
--    - Test 2: max_insert_block_size_bytes=160 (squash until 160 bytes, then emit)
--    - Test 3: min_insert_block_size_rows=1 AND min_insert_block_size_bytes=200 (emit when both met)
--    - Test 4: min_insert_block_size_rows=33 AND min_insert_block_size_bytes=8 (emit when both met)
-- 3. Verifies the number of parts created to confirm the new isEnoughSize() logic:
--    - min thresholds use AND: squashing continues until BOTH rows AND bytes are satisfied
--    - max thresholds use OR: squashing stops when EITHER rows OR bytes limit is reached

DROP TABLE IF EXISTS test_max_insert_rows_squashing;
DROP TABLE IF EXISTS test_max_insert_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_rows_less_than_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_bytes_less_than_rows_squashing;

CREATE TABLE test_max_insert_rows_squashing(
    id UInt64
)
Engine = MergeTree()
Order by id;

CREATE TABLE test_max_insert_bytes_squashing(
    id UInt64
)
Engine = MergeTree()
Order by id;

CREATE TABLE test_min_insert_rows_less_than_bytes_squashing(
    id UInt64
)
Engine = MergeTree()
Order by id;

CREATE TABLE test_min_insert_bytes_less_than_rows_squashing(
    id UInt64
)
Engine = MergeTree()
Order by id;

-- Set max_insert_block_size_rows smaller than max_insert_block_size_bytes
-- Expect 2 parts
SET max_insert_block_size_rows = 50;
SET max_insert_block_size_bytes = 1000000;
SET min_insert_block_size_rows = 1000000;
SET min_insert_block_size_bytes = 1000000;

INSERT INTO test_max_insert_rows_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10;

-- Set max_insert_block_size_bytes smaller than max_insert_block_size (rows)
-- Expect 5 parts
SET max_insert_block_size_rows = 1000000;
SET max_insert_block_size_bytes = 160;
SET min_insert_block_size_rows = 1000000;
SET min_insert_block_size_bytes = 1000000;

INSERT INTO test_max_insert_bytes_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10;

-- Disable max_insert_block_size_bytes,
-- set min_insert_block_size_rows < min_insert_block_size_bytes 
-- Expect 4 parts
SET max_insert_block_size_rows = 1000000;
SET max_insert_block_size_bytes = 0;
SET min_insert_block_size_rows = 1;
SET min_insert_block_size_bytes = 200;

INSERT INTO test_min_insert_rows_less_than_bytes_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10;

-- Disable max_insert_block_size_bytes,
-- set min_insert_block_size_rows > min_insert_block_size_bytes 
-- Expect 3 parts
SET max_insert_block_size_rows = 1000000;
SET max_insert_block_size_bytes = 0;
SET min_insert_block_size_rows = 33;
SET min_insert_block_size_bytes = 8;

INSERT INTO test_min_insert_bytes_less_than_rows_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10;

SYSTEM FLUSH LOGS;

-- We expect to see 2 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_max_insert_rows_squashing' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_max_insert_rows_squashing SELECT%' 
    ORDER BY event_time DESC LIMIT 1
));

-- We expect to see 5 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_max_insert_bytes_squashing' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_max_insert_bytes_squashing SELECT%' 
    ORDER BY event_time DESC LIMIT 1
));

-- We expect to see 4 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_min_insert_rows_less_than_bytes_squashing' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_rows_less_than_bytes_squashing SELECT%' 
    ORDER BY event_time DESC LIMIT 1
));

-- We expect to see 20 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_min_insert_bytes_less_than_rows_squashing' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_bytes_less_than_rows_squashing SELECT%' 
    ORDER BY event_time DESC LIMIT 1
));

DROP TABLE IF EXISTS test_max_insert_rows_squashing;
DROP TABLE IF EXISTS test_max_insert_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_rows_less_than_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_bytes_less_than_rows_squashing;