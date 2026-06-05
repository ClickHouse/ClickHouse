-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
--
-- Companion to `04276_105647_ttl_group_by_no_infinite_remerge.sql`. That test
-- exercises a table with only `TTL ... GROUP BY`. This one combines `GROUP BY`
-- with a `RECOMPRESS` TTL whose codec matches the column's existing codec, so
-- `TTLRecompressMergeSelector::canConsiderPart` returns false (no codec
-- change), but the part still carries a `recompression_ttl` entry whose
-- `ttl_finished` flag is never set.
--
-- Before the follow-up fix, `TTLPartDropMergeSelector::canConsiderPart`
-- consulted `has_any_non_finished_ttls`, which is true whenever any
-- `moves_ttl` or `recompression_ttl` entry is non-finished. Combined with the
-- post-aggregation `part_max_ttl` left over from the `GROUP BY` rule, that
-- gate stayed open and the same `TTLDrop` merge was scheduled on every
-- scheduler tick. The fix routes this selector through
-- `has_any_non_finished_rows_affecting_ttls`, which restricts the check to
-- TTL kinds that actually contribute to `part_max_ttl`.

DROP TABLE IF EXISTS t_ttl_group_by_recompress_no_remerge;

CREATE TABLE t_ttl_group_by_recompress_no_remerge
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32 CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (key)
TTL ts + INTERVAL 3 MONTH GROUP BY key SET value = sum(value),
    ts + INTERVAL 1 YEAR  RECOMPRESS CODEC(ZSTD(3))
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

-- All rows are far past both TTL boundaries.
INSERT INTO t_ttl_group_by_recompress_no_remerge VALUES
    (1, '2020-01-01 00:00:00', 1),
    (1, '2020-02-01 00:00:00', 2),
    (1, '2020-03-01 00:00:00', 1),
    (1, '2020-04-01 00:00:00', 2);

-- Force a single TTL merge.
OPTIMIZE TABLE t_ttl_group_by_recompress_no_remerge FINAL;

SYSTEM FLUSH LOGS part_log;

CREATE TEMPORARY TABLE merges_snapshot AS
SELECT count() AS merges_after_optimize
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_recompress_no_remerge'
  AND event_type IN ('MergeParts', 'MergePartsStart');

-- Give the background scheduler a chance to fire spurious re-merges.
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;

SYSTEM FLUSH LOGS part_log;
SELECT
    (
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 't_ttl_group_by_recompress_no_remerge'
          AND event_type IN ('MergeParts', 'MergePartsStart')
    ) - (SELECT merges_after_optimize FROM merges_snapshot) AS extra_merges_after_settle;

-- Sanity check: exactly one active part with the aggregated row,
-- value = 1+2+1+2 = 6, rows = 1.
SELECT active, rows
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_recompress_no_remerge'
  AND active;

SELECT sum(value) FROM t_ttl_group_by_recompress_no_remerge;

DROP TABLE t_ttl_group_by_recompress_no_remerge;
