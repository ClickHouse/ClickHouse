-- The `parallel_full_sorting_merge` join algorithm must also produce the sharded pipeline with the legacy
-- analyzer (`enable_analyzer = 0`), not silently fall back to a single merge join. The legacy path builds the
-- pre-join sorts in `InterpreterSelectQuery`, which must mark them `is_sorting_for_merge_join` the same way
-- the analyzer path does, otherwise `optimizeParallelFullSortingMergeJoin` would not recognize them.
--
-- The join is on `a`/`b`, which are not the tables' `ORDER BY` key, so read-in-order does not apply and a
-- plain full sort is built (the case the scatter path rewrites). `max_threads = 4` fixes the shard count > 1
-- so the sharding path is exercised on any runner. Checking the pipeline (not just result equality) proves
-- the sharded merge join is actually chosen.

SET enable_analyzer = 0;

DROP TABLE IF EXISTS pfsmj_legacy_left;
DROP TABLE IF EXISTS pfsmj_legacy_right;

CREATE TABLE pfsmj_legacy_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_legacy_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts on each side so the read produces multiple streams.
INSERT INTO pfsmj_legacy_left SELECT number, number FROM numbers(0, 30000);
INSERT INTO pfsmj_legacy_left SELECT number, number FROM numbers(30000, 30000);
INSERT INTO pfsmj_legacy_right SELECT number, number * 2 FROM numbers(90000);

-- `parallel_full_sorting_merge` must scatter both sides by the hash of the join key (`ScatterByPartitionTransform`)
-- into a per-shard merge join. This transform only appears when the scatter path was applied, so it proves the
-- sharded pipeline is used with `enable_analyzer = 0`.
SELECT 'parallel_is_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_legacy_left AS l INNER JOIN pfsmj_legacy_right AS r ON l.a = r.b SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- Contrast: plain `full_sorting_merge` on the same query is a single merge join with no scatter.
SELECT 'plain_is_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_legacy_left AS l INNER JOIN pfsmj_legacy_right AS r ON l.a = r.b SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- The sharded result must match the `hash` algorithm.
SELECT 'results_match',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_legacy_left AS l INNER JOIN pfsmj_legacy_right AS r ON l.a = r.b SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_legacy_left AS l INNER JOIN pfsmj_legacy_right AS r ON l.a = r.b SETTINGS join_algorithm = 'hash');

DROP TABLE pfsmj_legacy_left;
DROP TABLE pfsmj_legacy_right;
