-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
--
-- Before the fix, a `MergeTree` table with `TTL ... GROUP BY` whose data was
-- already past the TTL boundary would be re-merged by the scheduler on every
-- tick, forever. Each tick produced a new part identical to the previous one.
--
-- The cause was in `TTLPartDropMergeSelector::canConsiderPart`, which picked
-- every part with `part_max_ttl <= current_time` without consulting
-- `has_any_non_finished_ttls`. After the first TTL aggregation the resulting
-- part has all of its TTL infos marked `finished`, but the selector did not
-- check that flag and re-picked the part on every scheduler tick.

DROP TABLE IF EXISTS t_ttl_group_by_no_infinite_remerge;

CREATE TABLE t_ttl_group_by_no_infinite_remerge
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL ts + INTERVAL 3 MONTH GROUP BY key SET value = sum(value)
SETTINGS
    -- Set well above realistic CI pool pressure so this test never trips either gate.
    -- max_number_of_merges_with_ttl_in_pool: per-table override of the
    -- server-wide default (2). Without this, parallel sibling tests in
    -- flaky-check can exhaust the pool; `MergeSelectorApplier::chooseMergesFrom`
    -- then skips `tryChooseTTLMerge` and the spurious-merge counter under-reports
    -- the buggy behavior, giving a false PASS on a buggy binary.
    -- min_parts_to_merge_at_once: closes the `tryChooseRegularMerge` fallback
    -- so it cannot fold this small test's parts and mask a missing TTL fold.
    max_number_of_merges_with_ttl_in_pool = 100,
    min_parts_to_merge_at_once = 100;

-- All four rows are far past the TTL boundary, so the GROUP BY rule fires
-- on the first merge and aggregates them into a single row per key.
INSERT INTO t_ttl_group_by_no_infinite_remerge VALUES
    (1, '2020-01-01 00:00:00', 1),
    (1, '2020-02-01 00:00:00', 2),
    (1, '2020-03-01 00:00:00', 1),
    (1, '2020-04-01 00:00:00', 2);

-- Force a single TTL merge.
OPTIMIZE TABLE t_ttl_group_by_no_infinite_remerge FINAL;

-- Snapshot the number of merges fired so far.
SYSTEM FLUSH LOGS part_log;

CREATE TEMPORARY TABLE merges_snapshot AS
SELECT count() AS merges_after_optimize
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_no_infinite_remerge'
  AND event_type IN ('MergeParts', 'MergePartsStart');

-- Give the background scheduler a chance to fire spurious re-merges.
-- Before the fix this window would contain several extra TTLDropMerge events.
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;

-- After the fix the part is marked `finished` and the selector skips it, so
-- the merge count should be identical to the snapshot above.
SYSTEM FLUSH LOGS part_log;
SELECT
    (
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 't_ttl_group_by_no_infinite_remerge'
          AND event_type IN ('MergeParts', 'MergePartsStart')
    ) - (SELECT merges_after_optimize FROM merges_snapshot) AS extra_merges_after_settle;

-- Sanity check: exactly one active part with the aggregated row,
-- value = 1+2+1+2 = 6, rows = 1.
SELECT active, rows
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_no_infinite_remerge'
  AND active;

SELECT sum(value) FROM t_ttl_group_by_no_infinite_remerge;

DROP TABLE t_ttl_group_by_no_infinite_remerge;
