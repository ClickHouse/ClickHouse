-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- The test asserts the local query-plan shape, which parallel replicas would change. Random
-- settings are excluded because the statistics gate decides based on the execution topology,
-- which randomized read/aggregation settings legitimately change.

-- Gate behavior of the grouped count-distinct rewrite across executions: statistics recorded by
-- one aggregation must not drive another aggregation's rewrite, a decision warmed by a wide
-- execution must not apply to a narrow one, and a favorable decision must not stay latched after
-- the data drifts to an unfavorable shape.

SET query_plan_rewrite_grouped_count_distinct = 1;
SET max_rows_to_group_by = 0;
-- The gate requires group keys shared across several reading streams; the thread count is pinned
-- so the table is read in the same number of streams on any machine.
SET max_threads = 4;

DROP TABLE IF EXISTS t_cd_gate;
CREATE TABLE t_cd_gate (k UInt32, s String, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, concat('s', toString(intHash64(number) % 3000)), concat('l', toString(intHash32(number) % 2000))
FROM numbers(1000000);

SELECT 'statistics of uniqExact(s) do not rewrite uniqExact(lc) on its first run';
SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k ORDER BY k LIMIT 2;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(lc) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%';

SELECT 'a decision warmed by a wide execution does not apply to a single-threaded one';
SET max_threads = 1;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%';
SET max_threads = 4;

SELECT 'the rewritten runs refresh the gate: unfavorable data drift turns the rewrite back off';
-- The inserted rows make the group keys millions and the per-key distinct sets singletons — the
-- shape where the rewrite loses. The first query after the insert still runs rewritten (the gate
-- decided from the stale entry), but it records the drifted shape under the created steps' keys,
-- and the next plan steps aside. The drift run must use the aggregate's output: wrapping it in
-- `count()` would let the unused-column removal drop the `uniqExact` entirely.
INSERT INTO t_cd_gate SELECT 10 + number, 's_unique', 'l' FROM numbers(2000000);
SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k ORDER BY k LIMIT 1;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s) FROM t_cd_gate GROUP BY k) WHERE explain LIKE '%Aggregating%';

DROP TABLE t_cd_gate;
