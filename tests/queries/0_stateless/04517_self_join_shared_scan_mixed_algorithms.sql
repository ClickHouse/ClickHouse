SET enable_analyzer = 1;
SET query_plan_optimize_self_join_shared_scan = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_shared_storage_snapshot_in_query = 1;
-- Pin the join order: a swapped self-join changes which side's columns must be a subset of the
-- other's, so whether the rewrite fires.
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_sjss_mixed;
CREATE TABLE t_sjss_mixed (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_mixed SELECT number, toString(number) FROM numbers(10);

-- `chooseJoinAlgorithm` walks `join_algorithm` in order and executes the first algorithm that
-- applies. The rewrite must not change which algorithm wins: with a merge-style algorithm listed
-- before a hash one the merge-style join is executed, so the rewrite must NOT fire (2 scans).

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'full_sorting_merge,hash'
);

-- Correctness of the untouched merge-style join.
SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'full_sorting_merge,hash';

-- Same with a partial-merge algorithm before a hash-family one.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'prefer_partial_merge,grace_hash'
);

-- grace_hash falls through to the next entry when it does not support the join, so a merge-style
-- fallback after it must also prevent the rewrite.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'grace_hash,full_sorting_merge'
);

-- A hash algorithm listed first always wins, so a later merge-style entry is unreachable and the
-- rewrite fires (1 scan + buffer replay).
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'hash,full_sorting_merge'
);

SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'hash,full_sorting_merge';

-- The deprecated `default` means `direct,hash`; direct never applies to a MergeTree build side,
-- so hash wins and the rewrite fires.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'default'
);

DROP TABLE t_sjss_mixed;
