-- Tags: distributed

-- `ORDER BY ... WITH FILL ... INTERPOLATE` over a distributed source with `serialize_query_plan = 1`.
--
-- The shard's plan carries the `ORDER BY` sort description, which `serializeSortDescription` used to
-- reject with "WITH FILL is not supported in serialized sort description", so every such query failed
-- once plan serialization was on (the CI `distributed plan` shard turns it on globally, which is why the
-- other distributed `WITH FILL` tests used to pin `serialize_query_plan = 0`). `WITH FILL` itself stays
-- on the initiator: `FillingStep` is not serializable and is added above the merge, so a shard only
-- returns its rows in order.
-- See https://github.com/ClickHouse/ClickHouse/issues/115527

DROP TABLE IF EXISTS t_05043;

CREATE TABLE t_05043 (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_05043 VALUES (10, 1), (20, 2), (30, 3), (40, 5);

SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;

-- Two shards read the same table, so every real row appears twice, while the fill rows (g = 0 and
-- g = 4) are generated once on the initiator and `INTERPOLATE` fills their `id`.
SELECT '--- two shards, WITH FILL INTERPOLATE ---';
SELECT g, id
FROM remote('127.0.0.{1,2}', currentDatabase(), t_05043)
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);

-- A shard that returns its rows out of the initiator's merge order would show up as fill rows in the
-- wrong place, so read the same data with a `WHERE` that leaves a gap in the middle too.
SELECT '--- two shards, gap in the middle ---';
SELECT g, id
FROM remote('127.0.0.{1,2}', currentDatabase(), t_05043)
WHERE g != 3
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);

-- Fill rows on an empty result are generated once on the initiator, not once per shard.
SELECT '--- two shards, fill an empty result ---';
SELECT count()
FROM (
    SELECT g
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_05043)
    WHERE g > 100000
    ORDER BY g WITH FILL FROM 0 TO 100
);

DROP TABLE t_05043;
