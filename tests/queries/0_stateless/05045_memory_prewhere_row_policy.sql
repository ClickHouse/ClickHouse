DROP TABLE IF EXISTS t_memory_row_policy;

CREATE TABLE t_memory_row_policy (k UInt64, v String) ENGINE = Memory SETTINGS compress = 1;

-- Several inserts, so the table consists of multiple blocks.
INSERT INTO t_memory_row_policy SELECT number, toString(number) FROM numbers(0, 100);
INSERT INTO t_memory_row_policy SELECT number, toString(number) FROM numbers(100, 100);

DROP ROW POLICY IF EXISTS policy_05045 ON t_memory_row_policy;
CREATE ROW POLICY policy_05045 ON t_memory_row_policy FOR SELECT USING k % 2 = 0 TO CURRENT_USER;

SELECT 'row-level filter alone';
SELECT count() FROM t_memory_row_policy;

SELECT 'row-level filter with WHERE';
SELECT v FROM t_memory_row_policy WHERE k < 10 ORDER BY k;
SELECT count() FROM t_memory_row_policy WHERE k >= 100;

SELECT 'row-level filter with explicit PREWHERE';
SELECT v FROM t_memory_row_policy PREWHERE k < 10 ORDER BY k;
SELECT count() FROM t_memory_row_policy PREWHERE v != '';

SELECT 'row-level filter eliminating everything';
SELECT count() FROM t_memory_row_policy WHERE k = 1;

DROP ROW POLICY policy_05045 ON t_memory_row_policy;
DROP TABLE t_memory_row_policy;
