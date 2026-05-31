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
-- 20000 expired keys (year 2000) + 20000 future keys (now + 1 HOUR).
-- With `merge_max_block_size = 8192` the merge runs across multiple blocks.
INSERT INTO t_ttl_group_by_mixed_blocks
SELECT
    number AS key,
    if(number < 20000,
       toDateTime('2000-01-01 00:00:00') + INTERVAL number SECOND,
       now() + INTERVAL 1 HOUR) AS ts,
    1 AS value
FROM numbers(40000);

-- TTL merge aggregates the expired keys and leaves the future ones alone.
OPTIMIZE TABLE t_ttl_group_by_mixed_blocks FINAL;

-- The post-merge `group_by_ttl[k].max` must reflect the surviving future
-- rows (`now() + 1 HOUR + 5 SECOND`), not just the aggregated past rows.
-- Before the fix this is a year-2000 timestamp; with the fix it is in the
-- future. Equivalently, the `finished` flag (not exposed in system.parts)
-- must be false — a future `max` forces it false in the new `finalize`.
SELECT toDateTime(arrayElement(group_by_ttl_info.max, 1)) > now() AS group_by_max_is_future
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_ttl_group_by_mixed_blocks'
  AND active;

DROP TABLE t_ttl_group_by_mixed_blocks;
