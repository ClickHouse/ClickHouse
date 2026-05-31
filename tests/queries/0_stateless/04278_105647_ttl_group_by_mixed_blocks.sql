-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
--
-- A second-order bug in the parent commit's fix in `TTLAggregationAlgorithm`.
-- `TTLAggregationAlgorithm::execute` runs once per block; the parent commit
-- set `new_ttl_info.ttl_finished = true` from inside `execute` as soon as
-- `new_ttl_info.max` was past the TTL boundary. With a part that has both
-- already-expired and not-yet-expired rows, an early block of expired-only
-- rows would set `ttl_finished = true`, and a later block of surviving
-- future rows would promote `new_ttl_info.max` past `current_time` via
-- `MergeTreeDataPartTTLInfo::update` — which never clears the flag. The
-- resulting `group_by_ttl` entry is then written with `max` in the future
-- and `finished = true`, and the narrowed merge-selector gate added on top
-- of that fix excludes the part from `TTLDrop` and `TTLDelete` forever, so
-- the previously-future rows never get aggregated when their TTL finally
-- expires.
--
-- The fix moves the `ttl_finished` decision into
-- `TTLAggregationAlgorithm::finalize`, which runs once per merged part
-- after every block has been processed, and recomputes the flag from the
-- final `new_ttl_info.max`.

DROP TABLE IF EXISTS t_ttl_group_by_mixed_blocks;

CREATE TABLE t_ttl_group_by_mixed_blocks
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL ts + INTERVAL 5 SECOND GROUP BY key SET value = sum(value)
SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;

-- One part with both already-expired and not-yet-expired rows.
-- 20000 expired keys (year 2000) + 20000 future keys (now + 10s).
-- With `merge_max_block_size = 8192` the merge runs across multiple blocks.
INSERT INTO t_ttl_group_by_mixed_blocks
SELECT
    number AS key,
    if(number < 20000,
       toDateTime('2000-01-01 00:00:00') + INTERVAL number SECOND,
       now() + INTERVAL 10 SECOND) AS ts,
    1 AS value
FROM numbers(40000);

-- First TTL merge: aggregates the expired keys; leaves the future keys alone.
-- Before the fix the resulting part is left with
-- `group_by_ttl[k].finished = true` even though 20000 rows are still in the
-- future.
OPTIMIZE TABLE t_ttl_group_by_mixed_blocks FINAL;

-- Wait for the future rows' TTL boundary to pass (10s out from INSERT,
-- plus 5s TTL → ~15s total from INSERT; the OPTIMIZE above ran promptly,
-- so 15s of sleep here is enough).
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;

-- Trigger another merge. With the fix, the previously-future rows are now
-- expired and the `TTLDelete` / `TTLDrop` selector picks the part, running
-- TTL aggregation a second time. Without the fix the part is marked
-- `finished` and the selector skips it; only a `RegularMerge` (or no merge
-- at all) runs.
OPTIMIZE TABLE t_ttl_group_by_mixed_blocks FINAL;

SYSTEM FLUSH LOGS part_log;

-- Count TTL-driven merges after the sleep. Before the fix this is 0 (the
-- selector gate excludes the part). With the fix it is at least 1.
SELECT count() > 0 AS second_ttl_merge_fired
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_mixed_blocks'
  AND event_type = 'MergeParts'
  AND merge_reason IN ('TTLDeleteMerge', 'TTLDropMerge')
  AND event_time >= now() - INTERVAL 30 SECOND;

DROP TABLE t_ttl_group_by_mixed_blocks;
