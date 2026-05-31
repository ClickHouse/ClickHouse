-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
--
-- The narrower selector gate from a follow-up on this PR
-- (`canConsiderPart` returning `has_any_non_finished_rows_affecting_ttls`)
-- correctly excludes already-finished parts from being TTL merge CENTERS,
-- so the scheduler does not re-pick them on every tick. But that gate was
-- also being consulted to decide whether to EXTEND a chosen range into a
-- neighbor — so a fresh insert that produced an unfinished part could not
-- be merged with an adjacent finished part by the TTL selector, even
-- though combining them in a single TTL pass is correct and cheaper than
-- waiting for the regular selector to catch up.
--
-- The fix splits the gate into `canConsiderPart` (still strict, for
-- center eligibility) and `canIncludeInRange` (looser, allowing
-- finished neighbors with a valid `general_ttl_info`). The center stays
-- bounded by the per-tick cooldown reasoning; the neighbor join is
-- bounded by the disjoint-set tracking inside the range constructor.
--
-- The test creates a finished part `A` (insert + OPTIMIZE → fully
-- aggregated), then a new unfinished part `B` (insert), then triggers
-- merge selection. Before the fix the only merge that combines them is
-- `RegularMerge`; with the fix at least one `TTLDeleteMerge` /
-- `TTLDropMerge` joins them.

DROP TABLE IF EXISTS t_ttl_drop_includes_finished_neighbors;

CREATE TABLE t_ttl_drop_includes_finished_neighbors
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL ts + INTERVAL 1 SECOND GROUP BY key SET value = sum(value)
SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;

-- Part A: 1000 already-expired keys. The first OPTIMIZE aggregates and
-- marks the part finished.
INSERT INTO t_ttl_drop_includes_finished_neighbors
SELECT number AS key, toDateTime('2000-01-01 00:00:00') AS ts, 1 AS value
FROM numbers(1, 1000);

OPTIMIZE TABLE t_ttl_drop_includes_finished_neighbors FINAL;

-- Snapshot the count of TTL-driven merges up to here.
SYSTEM FLUSH LOGS part_log;
CREATE TEMPORARY TABLE merges_snapshot AS
SELECT count() AS n
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_drop_includes_finished_neighbors'
  AND event_type = 'MergeParts'
  AND merge_reason IN ('TTLDeleteMerge', 'TTLDropMerge');

-- Part B: 1000 fresh already-expired keys. New insert -> unfinished
-- `group_by_ttl` entry.
INSERT INTO t_ttl_drop_includes_finished_neighbors
SELECT number AS key, toDateTime('2000-01-01 00:00:00') AS ts, 1 AS value
FROM numbers(1001, 1000);

-- Trigger merge selection. With the fix at least one TTL-driven merge
-- includes part A (the finished neighbor) along with part B (the
-- unfinished center). Without the fix the TTL selector refuses to extend
-- into part A and the merge happens as `RegularMerge` only.
OPTIMIZE TABLE t_ttl_drop_includes_finished_neighbors FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT
    (
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 't_ttl_drop_includes_finished_neighbors'
          AND event_type = 'MergeParts'
          AND merge_reason IN ('TTLDeleteMerge', 'TTLDropMerge')
    ) > (SELECT n FROM merges_snapshot) AS ttl_merge_included_finished_neighbor;

DROP TABLE t_ttl_drop_includes_finished_neighbors;
