-- Functions depending on the block they run on (`runningDifference`, `neighbor`, `rowNumberInBlock`, `blockSize`)
-- must be calculated before the `LIMIT`, in the main half of lazy materialization: in the lazy half they would
-- run on the rows that survived the `LIMIT` instead of the original blocks.
-- `query_plan_execute_functions_after_sorting` is disabled, because it moves such functions after sorting by itself.

SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;
SET query_plan_execute_functions_after_sorting = 0;
SET max_threads = 1, max_block_size = 1000;
SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS t_memory_lazy_block;
CREATE TABLE t_memory_lazy_block (k UInt64, payload String, other String) ENGINE = Memory;
INSERT INTO t_memory_lazy_block SELECT (number * 7919) % 10007, toString(number), 'o' || toString(number) FROM numbers(10007) SETTINGS max_block_size = 1000;

SELECT '-- explain';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT runningDifference(toInt64(payload)), other FROM t_memory_lazy_block ORDER BY k LIMIT 5) WHERE explain LIKE '%Lazily read columns%';

SELECT '-- results';
SELECT runningDifference(toInt64(payload)), neighbor(payload, 1), payload, other FROM t_memory_lazy_block ORDER BY k LIMIT 5;
SELECT runningDifference(toInt64(payload)), neighbor(payload, 1), payload, other FROM t_memory_lazy_block ORDER BY k LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT rowNumberInBlock(), blockSize(), length(payload), payload, other FROM t_memory_lazy_block WHERE k % 3 = 0 ORDER BY k DESC LIMIT 5;
SELECT rowNumberInBlock(), blockSize(), length(payload), payload, other FROM t_memory_lazy_block WHERE k % 3 = 0 ORDER BY k DESC LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP TABLE t_memory_lazy_block;
