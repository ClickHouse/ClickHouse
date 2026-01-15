-- Tags: no-async-insert
-- Tests how squashing combines blocks from INSERT...SELECT based on min/max thresholds
-- Test cases for insert select 100 numbers:
--    1. min_insert_block_size_rows=1 AND min_insert_block_size_bytes=200 (emit when both thresholds are met)
--    2. min_insert_block_size_rows=33 AND min_insert_block_size_bytes=8 (emit when both thresholds are met)
--    3. Data integrity verification

DROP TABLE IF EXISTS test_min_insert_rows_less_than_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_bytes_less_than_rows_squashing;

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

SYSTEM FLUSH LOGS part_log;

-- We expect to see 4 parts inserted
SELECT count() 
FROM system.part_log 
WHERE table = 'test_min_insert_rows_less_than_bytes_squashing' 
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND event_time > (now() - 120);

-- We expect to see 4 parts inserted
SELECT count()
FROM system.part_log 
WHERE table = 'test_min_insert_bytes_less_than_rows_squashing' 
AND event_type = 'NewPart' 
AND database = currentDatabase()
AND event_time > (now() - 120);

SELECT count() FROM test_min_insert_rows_less_than_bytes_squashing;
SELECT count() FROM test_min_insert_bytes_less_than_rows_squashing;

DROP TABLE IF EXISTS test_min_insert_rows_less_than_bytes_squashing;
DROP TABLE IF EXISTS test_min_insert_bytes_less_than_rows_squashing;