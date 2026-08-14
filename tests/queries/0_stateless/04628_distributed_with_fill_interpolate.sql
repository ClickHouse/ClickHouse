-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111728
-- ORDER BY ... WITH FILL ... INTERPOLATE (...) over a Distributed source with two or more shards
-- used to throw LOGICAL_ERROR "Invalid number of rows in Chunk": WITH FILL was applied on the shards
-- (which emit a mergeable-after-aggregation state), so the shard materialized the INTERPOLATE output
-- column and the initiator re-added it under the same name, leaving a same-named column unpopulated.
-- WITH FILL must run only on the initiator, over the fully merged stream.

DROP TABLE IF EXISTS t_04628;

CREATE TABLE t_04628 (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_04628 VALUES (10, 1), (20, 2), (30, 3), (40, 5);

SET prefer_localhost_replica = 0;

-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this test exercises the shard read, not plan serialization.
SET serialize_query_plan = 0;

-- Two shards (the same table read twice), so every real row appears twice, while the WITH FILL
-- suffix rows (g = 0 and g = 4) are generated once on the initiator and INTERPOLATE fills their id.
SELECT g, id
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04628)
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);

-- WITH FILL without INTERPOLATE over the same two shards on an empty base must generate the fill
-- rows only once (issue #111555): 100 rows for FILL FROM 0 TO 100, not 200 (once per shard).
SELECT count()
FROM (
    SELECT g
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_04628)
    WHERE g > 100000
    ORDER BY g WITH FILL FROM 0 TO 100
);

DROP TABLE t_04628;
