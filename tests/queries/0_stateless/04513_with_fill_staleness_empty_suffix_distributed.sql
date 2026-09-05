-- Regression test for a use-of-uninitialized-value in the WITH FILL suffix path.
-- When the fill constraints are satisfied but STALENESS leaves nothing to fill, the
-- suffix generated an empty (0-row) chunk of freshly cloned columns. Feeding that empty,
-- uninitialized column into a downstream MergingSortedTransform (as happens when reading
-- from a two-shard Distributed table) made the merge cursor read past the end of the
-- empty column. Found by the AST fuzzer under MSan.

DROP TABLE IF EXISTS t_with_fill_empty_suffix;
CREATE TABLE t_with_fill_empty_suffix (key Int) ENGINE = Memory;
INSERT INTO t_with_fill_empty_suffix VALUES (100);

DROP TABLE IF EXISTS d_with_fill_empty_suffix;
CREATE TABLE d_with_fill_empty_suffix AS t_with_fill_empty_suffix
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_with_fill_empty_suffix);

-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this exercises the two-shard merge, not plan serialization.
SELECT _shard_num FROM d_with_fill_empty_suffix ORDER BY _shard_num ASC WITH FILL TO 46 STALENESS 1
SETTINGS serialize_query_plan = 0;

DROP TABLE d_with_fill_empty_suffix;
DROP TABLE t_with_fill_empty_suffix;
