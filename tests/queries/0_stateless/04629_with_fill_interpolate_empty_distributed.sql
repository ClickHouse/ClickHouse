-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111547
-- ORDER BY ... WITH FILL ... INTERPOLATE (...) over a network merge of two or more empty sorted
-- streams (here a two-address remote() of an empty view) used to build a ragged chunk: the shard
-- materialized the INTERPOLATE output column and the initiator re-added it under the same name, so
-- FillingTransform (which locates interpolate columns by name) left the duplicate unwritten. On an
-- empty result the WITH FILL FROM..TO suffix then grew the other columns while that column stayed
-- empty, and FillingTransform::saveLastRow read past the end of the empty column (a debug-only
-- PODArray out-of-bounds). WITH FILL now runs only on the initiator over the merged stream, so the
-- interpolate column is materialized once and the block stays rectangular.
--
-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this exercises the shard read, not plan serialization.

SELECT n, inter
FROM remote('127.0.0.{1,2}', view(
    SELECT number AS inter, toFloat32(number / 10) AS n FROM numbers(10) WHERE 0))
ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 2 INTERPOLATE (inter AS 1023)
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
