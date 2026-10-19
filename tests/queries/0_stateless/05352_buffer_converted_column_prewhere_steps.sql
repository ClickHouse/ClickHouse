-- Tags: no-fasttest
-- no-fasttest: the Parquet format is unavailable in the fast test build.

-- Reading a Buffer whose destination declares a column differently logs a warning per read.
SET send_logs_level = 'error';
-- Several conditions reach the per-condition split only with this on; it is randomized.
SET enable_multiple_prewhere_read_steps = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP ROW POLICY IF EXISTS p05352 ON t05352_pq_pol_buf;
DROP TABLE IF EXISTS t05352_alter_buf;
DROP TABLE IF EXISTS t05352_alter_dst;
DROP TABLE IF EXISTS t05352_mt_buf;
DROP TABLE IF EXISTS t05352_mt_dst;
DROP TABLE IF EXISTS t05352_mem_buf;
DROP TABLE IF EXISTS t05352_mem_dst;
DROP TABLE IF EXISTS t05352_pq_buf;
DROP TABLE IF EXISTS t05352_pq_pol_buf;
DROP TABLE IF EXISTS t05352_pq_dst;
DROP TABLE IF EXISTS t05352_mrg_buf;
DROP TABLE IF EXISTS t05352_mrg_dst;
DROP TABLE IF EXISTS t05352_mrgsrc;

-- The destination's column is retyped after the Buffer was created from it.
CREATE TABLE t05352_alter_dst (k UInt8, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t05352_alter_buf AS t05352_alter_dst
    ENGINE = Buffer(currentDatabase(), t05352_alter_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);
INSERT INTO t05352_alter_dst VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
ALTER TABLE t05352_alter_dst MODIFY COLUMN v UInt64;

SELECT 'W1 MergeTree, retyped by ALTER';
SELECT k, v FROM t05352_alter_buf PREWHERE v > 1 AND v < 5 ORDER BY k;

-- The Buffer narrows the destination's columns, so the converted values (v = 2, 2, 44, 4, 1 and
-- w = 1, 1, 0, 3, 0) differ from the stored ones and a filter on the stored type is visible.
CREATE TABLE t05352_mt_dst (k UInt8, v UInt16, w UInt16) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t05352_mem_dst (k UInt8, v UInt16, w UInt16) ENGINE = Memory;
CREATE TABLE t05352_pq_dst (k UInt8, v UInt16, w UInt16) ENGINE = File(Parquet);
CREATE TABLE t05352_mrgsrc (k UInt8, v UInt16, w UInt16) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t05352_mrg_dst (k UInt8, v UInt16, w UInt16) ENGINE = Merge(currentDatabase(), '^t05352_mrgsrc$');
INSERT INTO t05352_mt_dst VALUES (1, 2, 1), (2, 258, 1), (3, 300, 0), (4, 4, 3), (5, 1, 256);
INSERT INTO t05352_mem_dst SELECT * FROM t05352_mt_dst;
INSERT INTO t05352_pq_dst SELECT * FROM t05352_mt_dst ORDER BY k;
INSERT INTO t05352_mrgsrc SELECT * FROM t05352_mt_dst;

CREATE TABLE t05352_mt_buf (k UInt8, v UInt8, w UInt8)
    ENGINE = Buffer(currentDatabase(), t05352_mt_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);
CREATE TABLE t05352_mem_buf (k UInt8, v UInt8, w UInt8)
    ENGINE = Buffer(currentDatabase(), t05352_mem_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);
CREATE TABLE t05352_pq_buf (k UInt8, v UInt8, w UInt8)
    ENGINE = Buffer(currentDatabase(), t05352_pq_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);
CREATE TABLE t05352_pq_pol_buf (k UInt8, v UInt8, w UInt8)
    ENGINE = Buffer(currentDatabase(), t05352_pq_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);
CREATE TABLE t05352_mrg_buf (k UInt8, v UInt8, w UInt8)
    ENGINE = Buffer(currentDatabase(), t05352_mrg_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

SELECT 'W2 MergeTree, range';
SELECT k, v FROM t05352_mt_buf PREWHERE v > 1 AND v < 5 ORDER BY k;
SELECT 'W3 MergeTree, IN and inequality';
SELECT k, v FROM t05352_mt_buf PREWHERE v IN (2, 4, 44) AND v != 4 ORDER BY k;
SELECT 'W4 MergeTree, bare columns';
SELECT k, v, w FROM t05352_mt_buf PREWHERE v AND w ORDER BY k;
SELECT 'W5 Memory';
SELECT k, v FROM t05352_mem_buf PREWHERE v > 1 AND v < 5 ORDER BY k;
SELECT 'W6 Parquet';
SELECT k, v FROM t05352_pq_buf PREWHERE v > 1 AND v < 5 ORDER BY k;
SELECT 'W7 Parquet, row policy';
CREATE ROW POLICY p05352 ON t05352_pq_pol_buf USING v > 1 AND v < 5 TO ALL;
SELECT k, v FROM t05352_pq_pol_buf ORDER BY k;
SELECT 'W8 Merge';
SELECT k, v FROM t05352_mrg_buf PREWHERE v > 1 AND v < 5 ORDER BY k;

SELECT 'C1 converted column not selected';
SELECT k FROM t05352_mt_buf PREWHERE v > 1 AND v < 5 ORDER BY k;
SELECT k FROM t05352_pq_buf PREWHERE v > 1 AND v < 5 ORDER BY k;
SELECT 'C2 one condition';
SELECT k, v FROM t05352_mt_buf PREWHERE v > 1 ORDER BY k;
SELECT 'C3 no split';
SELECT k, v FROM t05352_mt_buf PREWHERE v > 1 AND v < 5 ORDER BY k SETTINGS enable_multiple_prewhere_read_steps = 0;

DROP ROW POLICY p05352 ON t05352_pq_pol_buf;
DROP TABLE t05352_alter_buf;
DROP TABLE t05352_alter_dst;
DROP TABLE t05352_mt_buf;
DROP TABLE t05352_mt_dst;
DROP TABLE t05352_mem_buf;
DROP TABLE t05352_mem_dst;
DROP TABLE t05352_pq_buf;
DROP TABLE t05352_pq_pol_buf;
DROP TABLE t05352_pq_dst;
DROP TABLE t05352_mrg_buf;
DROP TABLE t05352_mrg_dst;
DROP TABLE t05352_mrgsrc;
