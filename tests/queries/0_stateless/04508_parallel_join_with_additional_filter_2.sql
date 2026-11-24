-- RIGHT/FULL joins with a residual ON condition emit their unmatched right rows through the parallel
-- non-joined path; check that parallel_hash matches plain hash. UInt32 keys use two-level maps
-- (bucket-partitioned emission); UInt16 keys use single-level maps (slot-partitioned emission).

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key UInt32, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (key UInt32, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 SELECT number, number * 4, concat('l', toString(number)) FROM numbers(6);
INSERT INTO t2 SELECT number, 10, concat('r', toString(number)) FROM numbers(12);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET parallel_hash_join_threshold = 1;
SET max_threads = 16;
SET max_block_size = 2;
SET parallel_non_joined_rows_processing = 1;

SELECT '---- RIGHT ANTI HASH';
SELECT t2.* FROM t1 RIGHT ANTI JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '---- RIGHT ANTI PARALLEL HASH';
SELECT t2.* FROM t1 RIGHT ANTI JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '---- RIGHT HASH';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '---- RIGHT PARALLEL HASH';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '---- FULL HASH';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '---- FULL PARALLEL HASH';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

DROP TABLE IF EXISTS t1_16;
DROP TABLE IF EXISTS t2_16;

CREATE TABLE t1_16 (key UInt16, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_16 (key UInt16, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1_16 SELECT number, number * 4, concat('l', toString(number)) FROM numbers(6);
INSERT INTO t2_16 SELECT number, 10, concat('r', toString(number)) FROM numbers(12);

SELECT '---- UINT16 RIGHT ANTI HASH';
SELECT t2_16.* FROM t1_16 RIGHT ANTI JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '---- UINT16 RIGHT ANTI PARALLEL HASH';
SELECT t2_16.* FROM t1_16 RIGHT ANTI JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '---- UINT16 RIGHT HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 RIGHT JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '---- UINT16 RIGHT PARALLEL HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 RIGHT JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '---- UINT16 FULL HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 FULL JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '---- UINT16 FULL PARALLEL HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 FULL JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

DROP TABLE t1_16;
DROP TABLE t2_16;

DROP TABLE t1;
DROP TABLE t2;
