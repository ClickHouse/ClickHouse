-- Tags: no-async-insert
-- Tests how squashing combines blocks from INSERT...SELECT based on min/max thresholds
-- Test cases for insert select 100 numbers:
--    1. max_insert_block_size_rows=100 (squash until 50 rows, then emit)
--    2. max_insert_block_size_bytes=164 (squash until 160 bytes, then emit)
--    3. min_insert_block_size_rows=1 AND min_insert_block_size_bytes=200 (emit when both thresholds are met)
--    4. min_insert_block_size_rows=33 AND min_insert_block_size_bytes=8 (emit when both thresholds are met)
--    5. Data integrity verification

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

-- Expect 100 parts
SET max_insert_block_size_bytes = 0;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;
SET max_insert_block_size_rows = 1;

INSERT INTO test_max_insert_rows_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10,use_strict_insert_block_limits=1;

-- Expect 5 parts
SET min_insert_block_size_bytes = 0;
SET max_insert_block_size_rows = 0;
SET min_insert_block_size_rows = 100000;
SET max_insert_block_size_bytes = 164;

INSERT INTO test_max_insert_bytes_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10,use_strict_insert_block_limits=1;

-- Expect 4 parts
SET max_insert_block_size_rows = 0;
SET max_insert_block_size_bytes = 0;
SET min_insert_block_size_rows = 1;
SET min_insert_block_size_bytes = 200;

INSERT INTO test_min_insert_rows_less_than_bytes_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10,use_strict_insert_block_limits=1;

-- Expect 3 parts
SET max_insert_block_size_rows = 0;
SET max_insert_block_size_bytes = 0;
SET min_insert_block_size_rows = 33;
SET min_insert_block_size_bytes = 8;

INSERT INTO test_min_insert_bytes_less_than_rows_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10,use_strict_insert_block_limits=1;

SYSTEM FLUSH LOGS query_log, part_log;

-- We expect to see 100 parts inserted
SELECT count()
FROM system.part_log 
WHERE table = 'test_max_insert_rows_squashing' 
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND (query_id = (
    SELECT argMax(query_id, event_time) 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_max_insert_rows_squashing SELECT%' 
    AND current_database = currentDatabase() 
));


-- We expect to see 5 parts inserted
SELECT count()
FROM system.part_log 
WHERE table = 'test_max_insert_bytes_squashing' 
AND event_type = 'NewPart' 
AND (query_id = (
    SELECT argMax(query_id, event_time)  
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_max_insert_bytes_squashing SELECT%' 
    AND current_database = currentDatabase() 
));

-- We expect to see 4 parts inserted
SELECT count() 
FROM system.part_log 
WHERE table = 'test_min_insert_rows_less_than_bytes_squashing' 
AND event_type = 'NewPart' 
AND (query_id = (
    SELECT argMax(query_id, event_time)  
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_rows_less_than_bytes_squashing SELECT%' 
    AND current_database = currentDatabase() 
));


-- We expect to see 4 parts inserted
SELECT count()
FROM system.part_log 
WHERE table = 'test_min_insert_bytes_less_than_rows_squashing' 
AND event_type = 'NewPart' 
AND (query_id = (
    SELECT argMax(query_id, event_time)  
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_bytes_less_than_rows_squashing SELECT%' 
    AND current_database = currentDatabase() 
));

SELECT count() FROM test_max_insert_rows_squashing;
SELECT count() FROM test_max_insert_bytes_squashing;
SELECT count() FROM test_min_insert_rows_less_than_bytes_squashing;
SELECT count() FROM test_min_insert_bytes_less_than_rows_squashing;

DROP TABLE IF EXISTS test_max_insert_rows_squashing;
DROP TABLE IF EXISTS test_max_insert_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_rows_less_than_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_bytes_less_than_rows_squashing;