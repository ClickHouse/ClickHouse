-- A plateau of equal worst keys in the top-K heap must not stop eviction when
-- enough strictly better keys remain: such a plateau lies outside the top-K and
-- is fully evictable.  Only the tie-set that straddles the capacity boundary is
-- protected.  Getting this wrong wedges the heap - the skip boundary stays
-- pinned at the plateau, nothing is ever skipped or evicted, and the heap grows
-- until it freezes - which makes the optimization silently depend on the order
-- rows happen to arrive in.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
-- CI randomizes group_by_top_k_optimization_observation_rows: a tiny window freezes the heap
-- before it can prune, which is what the assertions below check does not happen.
SET group_by_top_k_optimization_observation_rows = 65536;
-- One stream, so the assertions below describe a single heap.
SET max_threads = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS t_plateau_first;
DROP TABLE IF EXISTS t_plateau_last;

CREATE TABLE t_plateau_first (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_plateau_last (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Prefix mode (`ORDER BY a`, a prefix of `GROUP BY a, b`), so every distinct
-- `(a, b)` pushes another copy of its `a` into the heap and plateaus are the
-- normal case.  The two tables hold the same rows; they differ only in whether
-- the 1000 rows sharing the late-sorting prefix `a = 1000000` arrive first or
-- last.  In `t_plateau_first` those become a plateau of equal worst keys while
-- the 400 better prefixes accumulate behind it.
INSERT INTO t_plateau_first SELECT 1000000, number FROM numbers(1000);
INSERT INTO t_plateau_first SELECT number % 400, number FROM numbers(199000);

INSERT INTO t_plateau_last SELECT number % 400, number FROM numbers(199000);
INSERT INTO t_plateau_last SELECT 1000000, number FROM numbers(1000);

SELECT a, b, count() FROM t_plateau_first GROUP BY a, b ORDER BY a ASC LIMIT 10
SETTINGS log_comment = '04655_plateau_first' FORMAT Null;

SELECT a, b, count() FROM t_plateau_last GROUP BY a, b ORDER BY a ASC LIMIT 10
SETTINGS log_comment = '04655_plateau_last' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Both arrival orders must prune: rows skipped, keys evicted, and no heap frozen
-- for want of anything to reject.  Before the plateau was evictable the first
-- order reported 0 / 0 / 1 here.
SELECT
    log_comment,
    max(ProfileEvents['AggregationTopKRowsSkipped']) > 100000 AS skipped_many,
    max(ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS evicted_some,
    max(ProfileEvents['AggregationTopKHeapsFrozen']) AS frozen
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment IN ('04655_plateau_first', '04655_plateau_last')
GROUP BY log_comment
ORDER BY log_comment;

-- Results stay correct for either arrival order.
DROP TABLE IF EXISTS gt_plateau_first;
CREATE TABLE gt_plateau_first ENGINE = Memory EMPTY AS
SELECT a, b, count() AS c FROM t_plateau_first GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 25;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_plateau_first
SELECT a, b, count() AS c FROM t_plateau_first GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 25;
SET enable_group_by_top_k_optimization = 1;

SELECT 'plateau_first_matches_unoptimized';
SELECT count() FROM
(
    SELECT a, b, count() AS c FROM t_plateau_first GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 25
) AS o
FULL JOIN gt_plateau_first AS u USING (a, b)
WHERE o.c != u.c OR isNull(o.c) OR isNull(u.c);

DROP TABLE IF EXISTS gt_plateau_last;
CREATE TABLE gt_plateau_last ENGINE = Memory EMPTY AS
SELECT a, b, count() AS c FROM t_plateau_last GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 25;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_plateau_last
SELECT a, b, count() AS c FROM t_plateau_last GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 25;
SET enable_group_by_top_k_optimization = 1;

SELECT 'plateau_last_matches_unoptimized';
SELECT count() FROM
(
    SELECT a, b, count() AS c FROM t_plateau_last GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 25
) AS o
FULL JOIN gt_plateau_last AS u USING (a, b)
WHERE o.c != u.c OR isNull(o.c) OR isNull(u.c);

-- A plateau that really does own the boundary (every key ties) still cannot be
-- evicted.  Which groups such a query returns is nondeterministic - they all tie
-- on `a`, so the LIMIT picks arbitrarily - but every group it does return must
-- carry its complete count, i.e. the protected tie-set must never be evicted.
SELECT 'all_keys_tied_groups_complete';
SELECT count(), countIf(c = 2) FROM
(
    SELECT a, b, count() AS c
    FROM (SELECT 0::UInt64 AS a, number % 25000 AS b FROM numbers(50000))
    GROUP BY a, b ORDER BY a ASC LIMIT 5
);

DROP TABLE t_plateau_first;
DROP TABLE t_plateau_last;

DROP TABLE gt_plateau_first;
DROP TABLE gt_plateau_last;
