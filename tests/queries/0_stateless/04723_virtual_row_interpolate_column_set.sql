-- https://github.com/ClickHouse/ClickHouse/issues/111831

DROP TABLE IF EXISTS t_vrow_set;

CREATE TABLE t_vrow_set (k UInt32, s String) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_vrow_set;

-- Two parts: the virtual row is emitted per part as the merge boundary.
INSERT INTO t_vrow_set VALUES (1, '8');
INSERT INTO t_vrow_set VALUES (2, '8');

-- optimize_read_in_order is randomized by the test runner and with it disabled these queries
-- succeed regardless of the fix, so pin it or the test is vacuous. enable_parallel_replicas is
-- pinned off because the INTERPOLATE placeholder reaches the remote header independently of the
-- virtual row there, which is a separate defect this test must not depend on either way.
SET read_in_order_use_virtual_row = 1, optimize_read_in_order = 1, enable_parallel_replicas = 0;

SELECT k, s FROM t_vrow_set WHERE s IN ('8') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');
SELECT k, s FROM t_vrow_set WHERE s IN (SELECT '8') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');
SELECT k, s FROM t_vrow_set WHERE s NOT IN ('9') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');
SELECT k, s FROM t_vrow_set WHERE (s, s) IN (('8', '8')) ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');
SELECT k, s FROM t_vrow_set WHERE has(['8'], s) ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');
SELECT k, s FROM t_vrow_set WHERE s IN ('8') ORDER BY k DESC WITH FILL FROM 4 TO 0 STEP -1 INTERPOLATE (s AS '7');
SELECT k, s FROM t_vrow_set WHERE s IN ('8') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT DISTINCT k, s FROM t_vrow_set WHERE s IN ('8') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');

DROP TABLE t_vrow_set;
