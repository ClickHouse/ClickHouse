-- Tags: no-async-insert
-- no-async-insert: Test expects new part for each insert

-- Tests how input format parsers form blocks based on min/max thresholds via clickhouse-client
-- 1. Creates 4 test tables
-- 2. Inserts 8 rows (inline CSV format) via client with different block formation thresholds:
--    - Test 1: max_insert_block_size_bytes=8 (emit block when reaching 8 bytes). max_insert_block_size_bytes=16 (emit block when reaching 16 bytes)
--    - Test 2: min_insert_block_size_rows=2 AND min_insert_block_size_bytes=16 (emit when both met), min_insert_block_size_rows=2000 AND min_insert_block_size_bytes=16000
--    - Test 3: min_insert_block_size_rows=4 (emit when 4 rows accumulated)
--    - Test 4: min_insert_block_size_bytes=32 (emit when 32 bytes accumulated)
-- 3. Verifies the number of parts created to confirm the new isEnoughSize() logic:
--    - min thresholds use AND: both rows AND bytes must be satisfied
--    - max thresholds use OR: either rows OR bytes triggers block emission

DROP TABLE IF EXISTS test_max_insert_bytes;
DROP TABLE IF EXISTS test_min_insert_rows_bytes;
DROP TABLE IF EXISTS test_min_insert_rows;
DROP TABLE IF EXISTS test_min_insert_bytes;

CREATE TABLE test_max_insert_bytes(
    id UInt64
)
Engine = MergeTree()
Order by id;

CREATE TABLE test_min_insert_rows_bytes(
    id UInt64
)
Engine = MergeTree()
Order by id;

CREATE TABLE test_min_insert_rows(
    id UInt64
)
Engine = MergeTree()
Order by id;

CREATE TABLE test_min_insert_bytes(
    id UInt64
)
Engine = MergeTree()
Order by id;

-- TEST 1: max_insert_block_size_bytes=8 (emit block when reaching 8 bytes). max_insert_block_size_bytes=16 (emit block when reaching 16 bytes)

-- Set max_insert_block_size_bytes smaller than max_insert_block_size (rows)
SET max_insert_block_size_rows = 100000000;
SET max_insert_block_size_bytes = 8;
-- Turn off squashing
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

-- This will produce 8 blocks since each row is 8 bytes and max_insert_block_size_bytes is 8, so a block will be emitted for each row
INSERT INTO test_max_insert_bytes FORMAT CSV
1
2
3
4
5
6
7
8

SET max_insert_block_size_rows = 100000000;
SET max_insert_block_size_bytes = 16;
-- Turn off squashing
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

-- This will produce 4 blocks since each row is 8 bytes and max_insert_block_size_bytes is 16, so a block will be emitted for every 2 rows
INSERT INTO test_max_insert_bytes FORMAT CSV
10
20
30
40
50
60
70
80


-- TEST 2: min_insert_block_size_rows=2 AND min_insert_block_size_bytes=16 (emit when both met), min_insert_block_size_rows=2000 AND min_insert_block_size_bytes=16000

-- Disable max_insert_block_size_bytes and max_insert_block_size_rows to avoid interference with min thresholds
SET max_insert_block_size_bytes = 0;
SET max_insert_block_size_rows = 100000000;
-- Set min_insert_block_size_rows and min_insert_block_size_bytes to 2 and 16 so that blocks are formed by 2
SET min_insert_block_size_rows = 2;
SET min_insert_block_size_bytes = 16;

INSERT INTO test_min_insert_rows_bytes FORMAT CSV
1
2
3
4
5
6
7
8

SET max_insert_block_size_bytes = 0;
SET max_insert_block_size_rows  = 100000000;
-- rows and bytes are bigger than inserted data, so all rows should be in 1 block
SET min_insert_block_size_rows = 2000;
SET min_insert_block_size_bytes = 16000;

INSERT INTO test_min_insert_rows_bytes FORMAT CSV
10
20
30
40
50
60
70
80

-- TEST 3: min_insert_block_size_rows=4 (emit when 4 rows accumulated)

SET max_insert_block_size_bytes = 0;
SET max_insert_block_size_rows  = 100000000;
-- Disable min_insert_block_size_bytes
-- Set min_insert_block_size_rows to 4
SET min_insert_block_size_rows = 4;
SET min_insert_block_size_bytes = 0;

INSERT INTO test_min_insert_rows FORMAT CSV
1
2
3
4
5
6
7
8

SET max_insert_block_size_bytes = 16;
SET max_insert_block_size_rows  = 100000000;
SET min_insert_block_size_rows = 2;
SET min_insert_block_size_bytes = 0;
-- This should produce 4 parts
INSERT INTO test_min_insert_rows FORMAT CSV
10
20
30
40
50
60
70
80


-- TEST 4: min_insert_block_size_bytes=32 (emit when 32 bytes accumulated)

SET max_insert_block_size_bytes = 0;
SET max_insert_block_size_rows  = 100000000;
-- Disable min_insert_block_size_rows
-- Set min_insert_block_size_bytes to 32
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 32;

INSERT INTO test_min_insert_bytes FORMAT CSV
1
2
3
4
5
6
7
8

SET max_insert_block_size_bytes = 0;
SET max_insert_block_size_rows  = 100000000;
SET min_insert_block_size_rows = 2;
SET min_insert_block_size_bytes = 8;

-- This should produce 4 parts
INSERT INTO test_min_insert_bytes FORMAT CSV
10
20
30
40
50
60
70
80

SYSTEM FLUSH LOGS query_log, part_log;

-- We expect to see 8+4 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_max_insert_bytes' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id 
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_max_insert_bytes FORMAT CSV%' 
    AND type = 'QueryFinish'
    AND current_database = currentDatabase() 
));

-- We expect to see 4+1 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_min_insert_rows_bytes' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id  
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_rows_bytes FORMAT CSV%' 
    AND type = 'QueryFinish'
    AND current_database = currentDatabase() 
));

-- We expect to see 2+4 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_min_insert_rows' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id  
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_rows FORMAT CSV%' 
    AND type = 'QueryFinish'
    AND current_database = currentDatabase() 
));

-- We expect to see 2 parts inserted
SELECT count()  
FROM system.part_log 
WHERE table = 'test_min_insert_bytes' 
AND event_type = 'NewPart' 
AND (query_id IN (
    SELECT query_id  
    FROM system.query_log 
    WHERE query LIKE '%INSERT INTO test_min_insert_bytes FORMAT CSV%' 
    AND type = 'QueryFinish'
    AND current_database = currentDatabase() 
));

DROP TABLE test_max_insert_bytes;
DROP TABLE test_min_insert_rows_bytes;
DROP TABLE test_min_insert_rows;
DROP TABLE test_min_insert_bytes;

