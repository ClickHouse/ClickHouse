-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/106021#discussion_r3346238146
--
-- A `TTL ... DELETE WHERE` rule whose predicate matches no rows keeps a
-- zero-watermark `rows_where_ttl` entry: `{min=0,max=0,finished=false}`.
-- When the same part also has a finished expired `GROUP BY` TTL, the
-- zero `rows_where_ttl` entry must not make
-- `hasAnyNonFinishedRowsAffectingTTLs` true, otherwise `TTLPartDropMergeSelector`
-- can keep selecting the already-finished part on every scheduler tick.

DROP TABLE IF EXISTS t_ttl_delete_where_groupby_no_reloop;

CREATE TABLE t_ttl_delete_where_groupby_no_reloop
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL
    ts + INTERVAL 1 SECOND DELETE WHERE key = 0,
    ts + INTERVAL 1 SECOND GROUP BY key SET value = sum(value)
SETTINGS
    min_bytes_for_wide_part = 0,
    merge_with_ttl_timeout = 0,
    -- Set well above realistic CI pool pressure so this test never trips either gate.
    -- max_number_of_merges_with_ttl_in_pool: per-table override of the
    -- server-wide default (2). Without this, parallel sibling tests in
    -- flaky-check can exhaust the pool; `MergeSelectorApplier::chooseMergesFrom`
    -- then skips `tryChooseTTLMerge` and the assertion's
    -- `merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')` filter misses it.
    -- min_parts_to_merge_at_once: closes the `tryChooseRegularMerge` fallback
    -- so it cannot fold this small test's parts and mask a missing TTL fold.
    max_number_of_merges_with_ttl_in_pool = 100,
    min_parts_to_merge_at_once = 100;

INSERT INTO t_ttl_delete_where_groupby_no_reloop
SELECT number AS key, toDateTime('2000-01-01 00:00:00') AS ts, 1 AS value
FROM numbers(1, 500);

-- TTL merge: the `GROUP BY` rule is finished, while the never-matching
-- `DELETE WHERE` leaves a zero `rows_where_ttl` entry.
OPTIMIZE TABLE t_ttl_delete_where_groupby_no_reloop FINAL;

SYSTEM FLUSH LOGS part_log;
CREATE TEMPORARY TABLE snap AS
SELECT count() AS n
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_delete_where_groupby_no_reloop'
  AND event_type = 'MergeParts'
  AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge');

SELECT sleep(3) FORMAT Null;

SYSTEM FLUSH LOGS part_log;

SELECT
    (
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 't_ttl_delete_where_groupby_no_reloop'
          AND event_type = 'MergeParts'
          AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
    ) - (SELECT n FROM snap) AS spurious_ttl_merges_after_optimize;

DROP TABLE t_ttl_delete_where_groupby_no_reloop;
