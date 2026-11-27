-- The trivial-view pushdown ships the whole outer query (including an outer GROUP BY) to
-- the shards, so each shard's Aggregator would enforce max_rows_to_group_by independently
-- against only its own local rows, instead of StorageView::readImpl's canonical behavior of
-- fetching raw rows and running a single Aggregator on the initiator that enforces the limit
-- once over the global key set. For group_by_overflow_mode = 'any'/'break' the merge phase
-- does not re-apply the cap globally, so independent per-shard enforcement could let more
-- distinct groups through in total than the limit allows. The pushdown is therefore suppressed
-- whenever the outer query has a GROUP BY and max_rows_to_group_by is non-zero, regardless of
-- overflow mode (matching `useDataParallelAggregation`, which disables independent
-- aggregation for the same reason).
--
-- Tags: distributed

SET enable_analyzer = 1;
-- Pin the legacy EXPLAIN plan format: the checks below grep for the
-- "Convert VIEW subquery result to VIEW table structure" step, which the default
-- 'pretty' EXPLAIN format (explain_query_plan_default) does not print.
SET explain_query_plan_default = 'legacy';
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 04695_local;
DROP TABLE IF EXISTS 04695_dist;
DROP VIEW IF EXISTS 04695_view;

CREATE TABLE 04695_local (k UInt32, v UInt32)
ENGINE = MergeTree ORDER BY k;

CREATE TABLE 04695_dist AS 04695_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04695_local);

CREATE VIEW 04695_view AS SELECT k, v FROM 04695_dist;

INSERT INTO 04695_dist VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
SYSTEM FLUSH DISTRIBUTED 04695_dist;

SET optimize_trivial_view_pushdown_to_distributed = 1;

-- Baseline: no GROUP BY at all, pushdown fires as usual.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT k, v FROM 04695_view);

-- Outer GROUP BY but max_rows_to_group_by is unset (0, the default): still no reason to
-- suppress, pushdown fires.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT k, sum(v) FROM 04695_view GROUP BY k)
SETTINGS max_rows_to_group_by = 0;

-- Outer GROUP BY with max_rows_to_group_by set: pushdown is suppressed regardless of
-- group_by_overflow_mode, since 'throw' would also behave correctly here but the other
-- two modes would not.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT k, sum(v) FROM 04695_view GROUP BY k)
SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'any';

SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT k, sum(v) FROM 04695_view GROUP BY k)
SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'break';

SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT k, sum(v) FROM 04695_view GROUP BY k)
SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'throw';

-- An aggregate query with no GROUP BY keys (a single implicit global group) is unaffected
-- by max_rows_to_group_by, so the pushdown must not be suppressed for it.
SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
FROM (EXPLAIN SELECT sum(v) FROM 04695_view)
SETTINGS max_rows_to_group_by = 2;

-- Result correctness: identical whether the pushdown fires or is suppressed.
SELECT k, sum(v) FROM 04695_view GROUP BY k ORDER BY k
SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'any';
SET optimize_trivial_view_pushdown_to_distributed = 0;
SELECT k, sum(v) FROM 04695_view GROUP BY k ORDER BY k
SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'any';

DROP VIEW 04695_view;
DROP TABLE 04695_dist;
DROP TABLE 04695_local;
