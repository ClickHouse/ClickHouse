
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

SET max_insert_block_size = 1;
SET min_insert_block_size_rows = 1;
SET min_insert_block_size_bytes = 1;
SET max_execution_time = 300;

CREATE TABLE t0 (x UInt64, y Tuple(UInt64, UInt64) ) ENGINE = MergeTree ORDER BY x SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
SYSTEM STOP MERGES t0;
INSERT INTO t0 SELECT if(number % 2 = 0, 0, number) as x, (x, 0) from numbers(200) SETTINGS max_block_size = 1;

CREATE TABLE t1 (x UInt64, y Tuple(UInt64, UInt64) ) ENGINE = MergeTree ORDER BY x;

SET min_joined_block_size_bytes = 100;

SET join_algorithm = 'parallel_hash';
SELECT sum(ignore(*)) FROM t0 a FULL JOIN t1 b ON a.x = b.x FORMAT Null;
