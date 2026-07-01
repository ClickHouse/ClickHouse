-- `join_algorithm` is an ordered priority list: the first algorithm that supports the join is the one that
-- runs. Both `full_sorting_merge` and `parallel_full_sorting_merge` build the same `FullSortingMergeJoin`, so
-- the query-plan rewrite that shards the join into per-shard merge joins (`ScatterByPartitionTransform`) must
-- fire only when `parallel_full_sorting_merge` is the algorithm actually SELECTED, not merely present as a
-- lower-priority fallback. Otherwise `full_sorting_merge,parallel_full_sorting_merge` would silently run the
-- sharded (unordered) path even though `full_sorting_merge` was chosen first.
--
-- The join is on `k` (an integer, not the tables' `ORDER BY` key) so a plain full sort is built - the case the
-- scatter path rewrites - and integer keys are hash-compatible with the merge comparison, so sharding is not
-- disabled for a key-type reason. `max_threads = 4` keeps the shard count > 1 on any runner. Two parts per
-- side give multiple streams. Two `ScatterByPartitionTransform` (one per side) appear when sharding fires.

DROP TABLE IF EXISTS pfsmj_prio_left;
DROP TABLE IF EXISTS pfsmj_prio_right;

CREATE TABLE pfsmj_prio_left (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_prio_right (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO pfsmj_prio_left  SELECT number, number FROM numbers(0, 2000);
INSERT INTO pfsmj_prio_left  SELECT number, number FROM numbers(2000, 2000);
INSERT INTO pfsmj_prio_right SELECT number, number * 2 FROM numbers(0, 4000);

-- Analyzer path (enable_analyzer = 1).

-- `parallel_full_sorting_merge` selected -> sharded.
SELECT 'analyzer parallel_only_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- `parallel_full_sorting_merge` first in the list -> selected -> sharded.
SELECT 'analyzer parallel_first_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge,full_sorting_merge', max_threads = 4);

-- `full_sorting_merge` first -> selected -> NOT sharded, even though `parallel_full_sorting_merge` follows.
SELECT 'analyzer full_first_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge,parallel_full_sorting_merge', max_threads = 4);

-- Plain `full_sorting_merge` -> NOT sharded (baseline).
SELECT 'analyzer full_only_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- Legacy analyzer path: the selected-algorithm flag is set on both construction paths. `enable_analyzer`
-- cannot be changed inside a subquery, so set it at session level (as in `04494`) rather than in the
-- `EXPLAIN PIPELINE` SETTINGS.
SET enable_analyzer = 0;

SELECT 'legacy parallel_only_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

SELECT 'legacy full_first_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge,parallel_full_sorting_merge', max_threads = 4);

SET enable_analyzer = 1;

-- Regardless of the chosen execution, all variants must return the same (correct) result.
SELECT 'result_all_equal',
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge,parallel_full_sorting_merge', max_threads = 4)
  AND
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_prio_left AS l INNER JOIN pfsmj_prio_right AS r ON l.k = r.k SETTINGS join_algorithm = 'hash');

DROP TABLE pfsmj_prio_left;
DROP TABLE pfsmj_prio_right;
