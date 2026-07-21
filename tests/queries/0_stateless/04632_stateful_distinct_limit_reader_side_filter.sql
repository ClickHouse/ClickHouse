-- Regression companion to 04630 for https://github.com/ClickHouse/ClickHouse/pull/110188:
-- `tryPushDownLimit` must not set the `DistinctStep` limit hint when a stateful function
-- (e.g. `neighbor`, `logTrace`) is hidden in a reader-side filter - an explicit `PREWHERE` or a
-- row-level security policy - below the distinct, not only in a visible `ExpressionStep` / `FilterStep`.
-- `subtreeHasStatefulFunctions` now also inspects `SourceStepWithFilterBase` reader-side filters.
-- (At head the outer `LIMIT` transform cancels the scan at the same point as the distinct hint, so this
--  exercises the reader-side plan shape and guards its correctness rather than discriminating on a count.)

SET allow_deprecated_error_prone_window_functions = 1;
SET max_threads = 1;
SET max_block_size = 65536;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_04632;
CREATE TABLE t_04632 (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_04632 SELECT number, number % 10 FROM numbers(100);

-- `neighbor` in an explicit PREWHERE (always true: `neighbor(v, 1) >= 0`) sits below `DISTINCT v ... LIMIT`.
-- There are 10 distinct values of `v`, so the distinct output must stay correct and complete.
SELECT count() FROM (SELECT DISTINCT v FROM t_04632 PREWHERE neighbor(v, 1) >= 0 LIMIT 100) SETTINGS enable_analyzer = 0;
SELECT count() FROM (SELECT DISTINCT v FROM t_04632 PREWHERE neighbor(v, 1) >= 0 LIMIT 100) SETTINGS enable_analyzer = 1;

-- Same with the stateful function in a row-level security policy (always true: `logTrace('rp') = 0`).
DROP ROW POLICY IF EXISTS rp_04632 ON t_04632;
CREATE ROW POLICY rp_04632 ON t_04632 USING logTrace('rp') = 0 TO ALL;
SELECT count() FROM (SELECT DISTINCT v FROM t_04632 LIMIT 100) SETTINGS enable_analyzer = 0;
SELECT count() FROM (SELECT DISTINCT v FROM t_04632 LIMIT 100) SETTINGS enable_analyzer = 1;
DROP ROW POLICY rp_04632 ON t_04632;

DROP TABLE t_04632;
