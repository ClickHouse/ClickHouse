SET enable_analyzer = 1;
-- A swap would turn LEFT ANTI into RIGHT ANTI, which cannot short-circuit the left side.
SET query_plan_join_swap_table = 0;

-- `LEFT ANTI JOIN ... ON 1` with a non-empty right side: every left row matches and is suppressed.
SELECT 'left anti true, right non-empty';
SELECT count() FROM numbers(10) AS l LEFT ANTI JOIN (SELECT 1 AS r) AS rt ON 1;

-- With an empty right side all left rows are unmatched and must be emitted.
SELECT 'left anti true, right empty';
SELECT count() FROM numbers(10) AS l LEFT ANTI JOIN (SELECT 1 AS r WHERE 0) AS rt ON 1;

-- With a constant-false predicate all left rows are unmatched and must be emitted.
SELECT 'left anti false, right non-empty';
SELECT count() FROM numbers(10) AS l LEFT ANTI JOIN (SELECT 1 AS r) AS rt ON 0;

-- `RIGHT ANTI` must not short-circuit: its result depends on the left-side cardinality.
SELECT 'right anti true, left non-empty';
SELECT count() FROM numbers(10) AS l RIGHT ANTI JOIN (SELECT 1 AS r) AS rt ON 1;

SELECT 'right anti true, left empty';
SELECT count() FROM (SELECT 1 AS x WHERE 0) AS l RIGHT ANTI JOIN (SELECT 1 AS r) AS rt ON 1;

-- Once the right side is known to be non-empty, the result is provably empty and the left side
-- must be cancelled without being fully read.
SELECT 'left anti true short-circuits the left side';
SELECT count() FROM numbers(15000000) AS l LEFT ANTI JOIN (SELECT 1 AS r) AS rt ON 1
SETTINGS max_threads = 4, max_block_size = 65536, log_comment = '04613_left_anti_short_circuit';

SYSTEM FLUSH LOGS query_log;

-- The sources may produce only a few blocks before the cancellation propagates;
-- 2M rows is a generous bound, while a full scan of the left side would read 15M.
SELECT read_rows < 2000000
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04613_left_anti_short_circuit'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
