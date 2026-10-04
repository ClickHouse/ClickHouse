-- Tags: distributed

-- A single-shard `remote()` read ships the whole query, so with `serialize_query_plan = 1` the shipped plan
-- contains the `FillingStep` itself (with two or more shards the fill stays on the initiator instead).
-- That used to fail with "Method serialize is not implemented for Filling", and before that with
-- "WITH FILL is not supported in serialized sort description".
-- The shard holds all the rows here, so filling there is what the SQL-text path does too, and the result
-- must equal local execution.

DROP TABLE IF EXISTS t_fill_ser;

CREATE TABLE t_fill_ser (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_fill_ser VALUES (10, 1), (20, 2), (30, 3), (40, 5);

-- `SelectStreamFactory::createForShardImpl` builds and ships a `QueryPlan` only with the analyzer; the
-- old-analyzer run would silently fall back to shipping SQL text and never exercise the serialization.
SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
-- The row multiplicity below is per shard, so keep the reads out of parallel replicas: the
-- parallel-replicas job enables it in the profile, and `automatic_parallel_replicas_mode` is randomized.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SELECT '-- FROM 0 TO 6 INTERPOLATE, local';
SELECT g, id FROM t_fill_ser ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);
SELECT '-- FROM 0 TO 6 INTERPOLATE, one shard';
SELECT g, id FROM remote('127.0.0.1', currentDatabase(), t_fill_ser)
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);

SELECT '-- STEP 2, one shard';
SELECT g FROM remote('127.0.0.1', currentDatabase(), t_fill_ser) ORDER BY g WITH FILL STEP 2;

SELECT '-- DESC WITH FILL STALENESS, one shard';
SELECT g FROM remote('127.0.0.1', currentDatabase(), t_fill_ser) ORDER BY g DESC WITH FILL STALENESS -2;

-- Two shards: the fill stays on the initiator, so every real row appears twice while the fill rows
-- (g = 0 and g = 4) are generated once.
SELECT '-- FROM 0 TO 6 INTERPOLATE, two shards';
SELECT g, id FROM remote('127.0.0.{1,2}', currentDatabase(), t_fill_ser)
ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);

DROP TABLE t_fill_ser;
