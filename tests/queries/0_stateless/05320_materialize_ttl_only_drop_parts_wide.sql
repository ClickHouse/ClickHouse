-- `MATERIALIZE TTL` with `ttl_only_drop_parts` on Wide parts: drops only fully expired parts, and none with `materialize_ttl_recalculate_only`.
DROP TABLE IF EXISTS t_materialize_ttl_drop_wide;

CREATE TABLE t_materialize_ttl_drop_wide (id UInt64, value String, event_time DateTime('UTC'))
ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id TTL event_time + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 1;

SYSTEM STOP TTL MERGES t_materialize_ttl_drop_wide;

-- Partition 0 is fully expired, partition 1 is partially expired.
INSERT INTO t_materialize_ttl_drop_wide SELECT number, toString(number),
    if(number % 2 = 0 OR number < 5, toDateTime('2000-01-01 00:00:00', 'UTC'), toDateTime('2100-01-01 00:00:00', 'UTC'))
FROM numbers(10);
SELECT 'source part type', partition, part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_ttl_drop_wide' AND active ORDER BY partition;

ALTER TABLE t_materialize_ttl_drop_wide MATERIALIZE TTL SETTINGS mutations_sync = 1;
SELECT 'recalculate only', id % 2 AS p, count() FROM t_materialize_ttl_drop_wide GROUP BY p ORDER BY p;

ALTER TABLE t_materialize_ttl_drop_wide MODIFY SETTING materialize_ttl_recalculate_only = 0;
ALTER TABLE t_materialize_ttl_drop_wide MATERIALIZE TTL SETTINGS mutations_sync = 1;
SELECT 'drop expired parts', id % 2 AS p, count() FROM t_materialize_ttl_drop_wide GROUP BY p ORDER BY p;
SELECT 'part type', part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_ttl_drop_wide' AND active AND rows > 0;

DROP TABLE t_materialize_ttl_drop_wide;
