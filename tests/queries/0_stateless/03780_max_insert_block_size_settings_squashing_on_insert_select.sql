-- Tags: no-async-insert
-- Tests how squashing combines blocks from INSERT...SELECT based on min/max thresholds
-- Test cases for insert select 100 numbers:
--    1. max_insert_block_size_rows=23 
--    2. max_insert_block_size_bytes=164 
--    3. Data integrity verification

DROP TABLE IF EXISTS test_max_insert_rows_squashing;
DROP TABLE IF EXISTS test_max_insert_bytes_squashing;


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

-- Expect 4 parts
SET max_insert_block_size_bytes = 0;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 100000;
SET max_insert_block_size_rows = 23;

INSERT INTO test_max_insert_rows_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10,use_strict_insert_block_limits=1;

-- Expect 5 parts
SET min_insert_block_size_bytes = 0;
SET max_insert_block_size_rows = 0;
SET min_insert_block_size_rows = 100000;
SET max_insert_block_size_bytes = 164;

INSERT INTO test_max_insert_bytes_squashing SELECT number FROM numbers(100) SETTINGS max_block_size = 10,use_strict_insert_block_limits=1;

SYSTEM FLUSH LOGS part_log;

-- We expect to see 5 parts inserted
SELECT count()
FROM system.part_log 
WHERE table = 'test_max_insert_rows_squashing' 
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND event_time > (now() - 120);

-- We expect to see 5 parts inserted
SELECT count()
FROM system.part_log 
WHERE table = 'test_max_insert_bytes_squashing' 
AND event_type = 'NewPart'
AND database = currentDatabase()
AND event_time > (now() - 120);

SELECT count() FROM test_max_insert_rows_squashing;
SELECT count() FROM test_max_insert_bytes_squashing;

DROP TABLE IF EXISTS test_max_insert_rows_squashing;
DROP TABLE IF EXISTS test_max_insert_bytes_squashing;