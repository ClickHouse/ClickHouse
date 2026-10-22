-- The query condition cache for the `Memory` engine: the granules and blocks where no row satisfies
-- a condition are recorded and skipped by later queries. The entries must not outlive the data they describe.

SET use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1;
SET max_threads = 2, max_block_size = 100000;

DROP TABLE IF EXISTS t_memory_qcc;
DROP TABLE IF EXISTS t_memory_qcc_keep;

SELECT '-- the filter of the query and PREWHERE, before and after the data change';
CREATE TABLE t_memory_qcc (k UInt64, x UInt8, s String) ENGINE = Memory SETTINGS compress = 1;
-- Three blocks of 30000 rows, the matching rows in a single granule of the middle block.
INSERT INTO t_memory_qcc SELECT number, number BETWEEN 40000 AND 40009, toString(number) FROM numbers(90000) SETTINGS max_block_size = 30000;

-- Twice each: the second query finds the entries written by the first one.
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;

-- A mutation changes the blocks.
ALTER TABLE t_memory_qcc UPDATE x = 1 WHERE k % 10000 = 1;
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;

ALTER TABLE t_memory_qcc DELETE WHERE k < 30000;
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;

-- So does TRUNCATE followed by an INSERT of other data.
TRUNCATE TABLE t_memory_qcc;
INSERT INTO t_memory_qcc SELECT number, number % 1000 = 0, toString(number) FROM numbers(90000) SETTINGS max_block_size = 30000;
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;

-- New blocks have no entries yet.
INSERT INTO t_memory_qcc SELECT number, 1, toString(number) FROM numbers(90000, 10);
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;

SELECT '-- ARRAY JOIN changes the number of rows';
SELECT count() FROM t_memory_qcc ARRAY JOIN range(k % 3) AS a WHERE x = 1 AND a = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_memory_qcc ARRAY JOIN range(k % 3) AS a WHERE x = 1 AND a = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_memory_qcc ARRAY JOIN range(k % 3) AS a WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- an explicit PREWHERE is not a part of the filter of the query';
SELECT count() FROM t_memory_qcc PREWHERE k < 1000 WHERE x = 1;
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_memory_qcc PREWHERE x = 1 WHERE k >= 1000;
SELECT count() FROM t_memory_qcc PREWHERE x = 1;

SELECT '-- a row policy and the TopN filter remove rows before PREWHERE';
CREATE ROW POLICY IF NOT EXISTS 05322_memory_qcc_policy ON t_memory_qcc USING k >= 50000 TO ALL;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 AND s != '' SETTINGS optimize_move_to_prewhere = 1;
DROP ROW POLICY 05322_memory_qcc_policy ON t_memory_qcc;
SELECT count(), sum(length(s)) FROM t_memory_qcc WHERE x = 1 AND s != '' SETTINGS optimize_move_to_prewhere = 1;

SELECT k, s FROM t_memory_qcc WHERE x = 1 AND s LIKE '%0%' ORDER BY k DESC LIMIT 3 SETTINGS optimize_move_to_prewhere = 1, use_top_k_dynamic_filtering = 1;
SELECT count() FROM t_memory_qcc WHERE x = 1 AND s LIKE '%0%' SETTINGS optimize_move_to_prewhere = 1;

SELECT '-- the oldest blocks are removed by max_rows_to_keep';
CREATE TABLE t_memory_qcc_keep (k UInt64, x UInt8, s String) ENGINE = Memory SETTINGS max_rows_to_keep = 20000;
INSERT INTO t_memory_qcc_keep SELECT number, 0, toString(number) FROM numbers(10000);
INSERT INTO t_memory_qcc_keep SELECT number, 0, toString(number) FROM numbers(10000, 10000);
SELECT count() FROM t_memory_qcc_keep WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc_keep WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;
-- The first block goes away, and the new one takes its place in the list of blocks.
INSERT INTO t_memory_qcc_keep SELECT number, 1, toString(number) FROM numbers(20000, 10000);
SELECT count() FROM t_memory_qcc_keep;
SELECT count() FROM t_memory_qcc_keep WHERE x = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(length(s)) FROM t_memory_qcc_keep WHERE x = 1 SETTINGS optimize_move_to_prewhere = 1;

SELECT '-- the results are the same without the cache';
SELECT count() FROM t_memory_qcc WHERE x = 1 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_memory_qcc_keep WHERE x = 1 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_memory_qcc;
DROP TABLE t_memory_qcc_keep;
