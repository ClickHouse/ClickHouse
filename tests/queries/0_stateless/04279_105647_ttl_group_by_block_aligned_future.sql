-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
--
-- Sharper variant of `04278_..._mixed_blocks`. The 04278 test mixes the
-- expired and future rows so the merge always processes at least one block
-- that contains both kinds. With aggregation firing inside that block,
-- `some_rows_were_aggregated` flips to true and the post-block recalc loop
-- runs over `result_columns` — which incidentally includes the surviving
-- future rows, so `new_ttl_info.max` advances to the future. The test then
-- passes even with the original gated recalc.
--
-- This test arranges the expired and future rows into separate, block-
-- aligned ranges of the primary key so that with the default
-- `merge_max_block_size = 8192`:
--
--   Block 1: keys 0..8191      — all expired (year 2000).
--   Block 2: keys 8192..16383  — all future (`now() + 1 HOUR`).
--
-- Block 2 contains zero rows the TTL aggregator wants to touch, so
-- `some_rows_were_aggregated` stays false. Before the fix the post-block
-- recalc was gated on that flag, so the surviving future rows never
-- contributed to `new_ttl_info.max`. `finalize` then persisted
-- `{max = past, finished = true}` and the part was permanently invisible
-- to the TTL selectors.
--
-- The fix removes the gate: the post-block recalc runs unconditionally
-- so future rows always promote `new_ttl_info.max`.

DROP TABLE IF EXISTS t_ttl_group_by_block_aligned_future;

CREATE TABLE t_ttl_group_by_block_aligned_future
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL ts + INTERVAL 5 SECOND GROUP BY key SET value = sum(value)
SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;

INSERT INTO t_ttl_group_by_block_aligned_future
SELECT
    number AS key,
    if(number < 8192,
       toDateTime('2000-01-01 00:00:00') + INTERVAL number SECOND,
       now() + INTERVAL 1 HOUR) AS ts,
    1 AS value
FROM numbers(16384);

OPTIMIZE TABLE t_ttl_group_by_block_aligned_future FINAL;

-- The part's `group_by_ttl` watermark must reflect the surviving future
-- rows, not just the aggregated past rows. `groupArray` is used so we can
-- pick out the single `group_by` entry's max regardless of ordering.
SELECT toDateTime(arrayElement(group_by_ttl_info.max, 1)) > now() AS group_by_max_is_future
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_block_aligned_future'
  AND active;

DROP TABLE t_ttl_group_by_block_aligned_future;
