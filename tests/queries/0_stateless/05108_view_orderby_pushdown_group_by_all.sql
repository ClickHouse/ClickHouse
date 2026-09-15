-- Tags: distributed

-- `pushOrderByIntoView` injects the outer `ORDER BY ... LIMIT` into the stored query of a view over
-- a `Distributed` table. It must refuse a view whose body aggregates or limits per group, because
-- under the pushdown that aggregation or per-group limit runs per shard over the shard-local
-- top-N instead of once over all rows. `GROUP BY ALL` leaves the `groupBy()` expression list empty
-- and only raises the `group_by_all` flag, so the list check alone does not see it: an aggregating
-- view body was rewritten and each shard got its own `Top-K` on the group keys.
-- The `LIMIT ... BY ALL` / `ORDER BY ALL` forms are also checked through their flags now, the same
-- way `StorageView::tryGetTrivialViewUnderlyingStorage` does it.

SET enable_analyzer = 1;

-- Target `pushOrderByIntoView` and not the unrelated trivial-view pushdown to `Distributed`.
SET optimize_trivial_view_pushdown_to_distributed = 0;

DROP TABLE IF EXISTS local_05108;
DROP TABLE IF EXISTS dist_05108;

CREATE TABLE local_05108 (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO local_05108 SELECT number, number % 10 FROM numbers(100);

CREATE TABLE dist_05108 AS local_05108
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_05108, id);

-- A plain projection view: the pushdown fires, the per-shard sort is merged by the coordinator.
-- This control makes the "no pushdown" lines below non-vacuous.
CREATE VIEW view_plain_05108 AS SELECT id, k FROM dist_05108;

SELECT 'plain view keeps pushdown:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM view_plain_05108 ORDER BY id DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- `GROUP BY ALL`: before the flag was checked, the injected inner `ORDER BY k DESC LIMIT 3` made
-- every shard aggregate with a `Top-K: limit=3` on its own rows, so a shard could drop a group
-- that belongs to the global top-3. The aggregation must stay a plain per-shard `Aggregating`
-- whose states the coordinator merges before sorting.
CREATE VIEW view_group_by_all_05108 AS SELECT k, count() AS c FROM dist_05108 GROUP BY ALL;

SELECT 'GROUP BY ALL: no per-shard Top-K:',
    (SELECT count() FROM (EXPLAIN SELECT k FROM view_group_by_all_05108 ORDER BY k DESC LIMIT 3)
     WHERE explain LIKE '%Top-K: limit=%') AS shard_local_top_k;

-- The explicit `GROUP BY k` form was already rejected by the `groupBy()` check. It is the twin of
-- the flag-only form above and shows the plan an aggregating view has to keep.
CREATE VIEW view_group_by_05108 AS SELECT k, count() AS c FROM dist_05108 GROUP BY k;

SELECT 'explicit GROUP BY: no per-shard Top-K:',
    (SELECT count() FROM (EXPLAIN SELECT k FROM view_group_by_05108 ORDER BY k DESC LIMIT 3)
     WHERE explain LIKE '%Top-K: limit=%') AS shard_local_top_k;

-- `GROUP BY ALL WITH TOTALS` carries the `group_by_with_totals` marker on top of the flag.
CREATE VIEW view_group_by_all_totals_05108 AS SELECT k, count() AS c FROM dist_05108 GROUP BY ALL WITH TOTALS;

SELECT 'GROUP BY ALL WITH TOTALS: no per-shard Top-K:',
    (SELECT count() FROM (EXPLAIN SELECT k FROM view_group_by_all_totals_05108 ORDER BY k DESC LIMIT 3)
     WHERE explain LIKE '%Top-K: limit=%') AS shard_local_top_k;

-- The counts of the view must be the ones of both shards together (10 rows per group per shard).
SELECT 'GROUP BY ALL counts stay global:',
    (SELECT groupArray(c) FROM (SELECT k, c FROM view_group_by_all_05108 ORDER BY k DESC LIMIT 3)) AS counts;

-- `LIMIT 1 BY ALL` (`limit_by_all` + `limitByLength()`) and its explicit twin `LIMIT 1 BY k`: the
-- per-group limit of the view body must be applied by the view over all rows, so the injected
-- inner `ORDER BY ... LIMIT` must not appear below it. The parser happens to leave an empty
-- `limitBy()` list behind for the `ALL` form, so it was already rejected before the flags were
-- checked; these lines are the regression guard for that. The pushdown, when it fires, sorts once
-- per shard on top of the coordinator sort, which is what the plain view above shows.
CREATE VIEW view_limit_by_all_05108 AS SELECT id, k FROM dist_05108 LIMIT 1 BY ALL;
CREATE VIEW view_limit_by_05108 AS SELECT id, k FROM dist_05108 LIMIT 1 BY k;

SELECT 'plain view sorts per shard:',
    (SELECT count() > 1 FROM (EXPLAIN SELECT id FROM view_plain_05108 ORDER BY id DESC LIMIT 3)
     WHERE explain LIKE '%Sorting%') AS sorting_steps;

SELECT 'LIMIT BY ALL sorts only once:',
    (SELECT count() FROM (EXPLAIN SELECT id FROM view_limit_by_all_05108 ORDER BY id DESC LIMIT 3)
     WHERE explain LIKE '%Sorting%') AS sorting_steps;

SELECT 'explicit LIMIT BY sorts only once:',
    (SELECT count() FROM (EXPLAIN SELECT id FROM view_limit_by_05108 ORDER BY id DESC LIMIT 3)
     WHERE explain LIKE '%Sorting%') AS sorting_steps;

DROP VIEW view_plain_05108;
DROP VIEW view_group_by_all_05108;
DROP VIEW view_group_by_05108;
DROP VIEW view_group_by_all_totals_05108;
DROP VIEW view_limit_by_all_05108;
DROP VIEW view_limit_by_05108;
DROP TABLE dist_05108;
DROP TABLE local_05108;
