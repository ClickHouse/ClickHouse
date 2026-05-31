-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
--
-- Variant flagged by `clickhouse-gh[bot]` on PR #106021: an earlier fix in
-- `TTLAggregationAlgorithm::finalize` falls back to `old_ttl_info` when
-- `new_ttl_info.max == 0`. The fallback was meant to handle "the merge
-- processed no surviving rows so don't regress the watermark to zero",
-- but a `TTL ... DELETE` rule next to a `TTL ... GROUP BY` rule can
-- delete every row before `TTLAggregationAlgorithm::execute` ever sees
-- non-empty input. With `remove_empty_parts = 0` the resulting empty
-- part stays around with the old `group_by_ttl[k]` carrying
-- `finished = false` and an expired `max`. The new
-- `hasAnyNonFinishedRowsAffectingTTLs` gate then returns true on every
-- scheduler tick and `TTLPartDropMergeSelector` keeps re-picking the
-- empty part, reproducing the exact infinite loop the issue describes.
--
-- The fix in `finalize` distinguishes the two `new_ttl_info.max == 0`
-- cases by inspecting a new `any_surviving_row_seen` flag on
-- `TTLAggregationAlgorithm`. The flag is set inside the per-block
-- post-aggregation recalc loop in `execute`. If no block ever produced
-- a surviving row for this rule on this part, the rule has no more
-- inputs and `finalize` writes a finished zero entry; otherwise it
-- falls back to `old_ttl_info`. `data_part->rows_count` cannot be
-- used here because `MergedBlockOutputStream::finalizePart` only sets
-- it AFTER the TTL pipeline has already run, so it is still zero at
-- `finalize` time and would force the always-finished branch.

DROP TABLE IF EXISTS t_ttl_delete_groupby_empty_part_no_reloop;

CREATE TABLE t_ttl_delete_groupby_empty_part_no_reloop
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL
    ts + INTERVAL 1 SECOND DELETE,
    ts + INTERVAL 1 SECOND GROUP BY key SET value = sum(value)
SETTINGS
    min_bytes_for_wide_part = 0,
    merge_with_ttl_timeout = 0,
    remove_empty_parts = 0;

INSERT INTO t_ttl_delete_groupby_empty_part_no_reloop
SELECT number AS key, toDateTime('2000-01-01 00:00:00') AS ts, 1 AS value
FROM numbers(1, 500);

-- TTL merge: DELETE removes every row; the GROUP BY rule's
-- `TTLAggregationAlgorithm::execute` is called only on empty blocks.
-- Before the fix this leaves `group_by_ttl[k] = {max=past, finished=0}`
-- in the resulting empty part.
OPTIMIZE TABLE t_ttl_delete_groupby_empty_part_no_reloop FINAL;

-- Snapshot how many TTL-driven merges fired through the OPTIMIZE FINAL.
SYSTEM FLUSH LOGS part_log;
CREATE TEMPORARY TABLE snap AS
SELECT count() AS n
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_delete_groupby_empty_part_no_reloop'
  AND event_type = 'MergeParts'
  AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge');

-- Give the background scheduler ~3 seconds to fire any spurious re-merges
-- on the resulting empty part.
SELECT sleep(3) FORMAT Null;

SYSTEM FLUSH LOGS part_log;

-- After the fix the empty part's `group_by_ttl[k]` is marked finished,
-- so `hasAnyNonFinishedRowsAffectingTTLs` returns false and neither
-- `TTLPartDropMergeSelector` nor `TTLRowDeleteMergeSelector` picks it
-- up. The TTL-merge count is identical to the snapshot.
SELECT
    (
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 't_ttl_delete_groupby_empty_part_no_reloop'
          AND event_type = 'MergeParts'
          AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
    ) - (SELECT n FROM snap) AS spurious_ttl_merges_after_optimize;

DROP TABLE t_ttl_delete_groupby_empty_part_no_reloop;
