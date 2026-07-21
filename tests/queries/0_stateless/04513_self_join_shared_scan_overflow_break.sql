-- Under `join_overflow_mode = 'break'` the build side may stop consuming its input when
-- `max_rows_in_join` / `max_bytes_in_join` is reached, so the shared buffer would hold only a
-- prefix of the scan and the probe side would lose rows beyond the join's soft limit
-- (e.g. the preserved side of a LEFT JOIN). The rewrite must not fire in that case.

SET enable_analyzer = 1; -- the rewrite requires the analyzer
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test
SET enable_join_runtime_filters = 0; -- a runtime filter makes the scan non-plain and blocks the rewrite
SET enable_parallel_replicas = 0; -- reading with parallel replicas blocks the rewrite
SET enable_shared_storage_snapshot_in_query = 1; -- the rewrite requires both scans to share one storage snapshot
SET query_plan_join_swap_table = 0; -- under break, a swap changes which side gets truncated
SET query_plan_optimize_join_order_randomize = 0; -- join order randomization may swap the sides

DROP TABLE IF EXISTS t_sjss_break;
CREATE TABLE t_sjss_break (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_break SELECT number, toString(number) FROM numbers(100);

-- Plan shape: `join_overflow_mode = 'break'` with a row limit keeps two scans and no buffer.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_rows_in_join = 10, join_overflow_mode = 'break'
);

-- Same with a byte limit.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break'
);

-- Every left-side row must survive: the LEFT JOIN's soft limit may only drop matches,
-- never rows of the preserved side. The small `max_block_size` splits the scan into many
-- blocks so the build side actually stops early at the limit.
SELECT count() FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
SETTINGS max_rows_in_join = 10, join_overflow_mode = 'break', max_block_size = 10;

-- `join_overflow_mode = 'throw'` (the default) with a limit still allows the rewrite.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_rows_in_join = 1000, join_overflow_mode = 'throw'
);

-- 'break' without any limit set is inert, the rewrite may fire.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_rows_in_join = 0, max_bytes_in_join = 0, join_overflow_mode = 'break'
);

DROP TABLE t_sjss_break;
