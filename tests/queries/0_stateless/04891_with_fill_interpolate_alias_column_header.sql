-- Tags: shard

-- The `Before INTERPOLATE` step used to pass every column *available* to it through as an output, and a
-- step's available columns are all the nodes of the preceding dags, intermediates included. That put those
-- intermediates into the header of the `Filling` step, so a query whose `ALIAS` column had been inlined
-- into its defining expression got a header wider than the same query with the column left alone:
-- `v * 2` contributes its `2`, `a_v` does not. A distributed query is planned from the un-inlined tree on
-- the initiator and executed from the inlined one on the shard, so the two headers met and could not be
-- reconciled:
--   Number of columns doesn't match (source: 5 and result: 4). (NUMBER_OF_COLUMNS_DOESNT_MATCH)

SET enable_analyzer = 1;

-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this test exercises the header, not plan serialization.
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_fill_alias;
DROP TABLE IF EXISTS t_fill_alias_dist;

CREATE TABLE t_fill_alias (k UInt32, v Int64, w Int64, a_v Int64 ALIAS v * 2, a_cast Int32 ALIAS abs(v))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_fill_alias VALUES (0, 5, 1), (4, 7, 1), (8, 9, 1);

CREATE TABLE t_fill_alias_dist AS t_fill_alias
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t_fill_alias, rand());

SELECT 'local';
SELECT k, a_v FROM t_fill_alias ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_v AS a_v);

SELECT 'parallel replicas, shipping a plan';
SELECT k, a_v FROM t_fill_alias ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_v AS a_v)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 1;

SELECT 'parallel replicas, shipping SQL';
SELECT k, a_v FROM t_fill_alias ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_v AS a_v)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 0;

-- The `Distributed` path has always inlined, so this shape was broken there before the parallel-replicas
-- paths started inlining too.
SELECT 'Distributed, two shards';
SELECT k, a_v FROM t_fill_alias_dist ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_v AS a_v);

-- A body whose declared type forces a cast contributes an intermediate of its own.
SELECT 'a body that carries a cast';
SELECT k, a_cast FROM t_fill_alias_dist ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_cast AS a_cast);

-- Interpolating by an expression over the alias, rather than the alias itself.
SELECT 'interpolating by an expression';
SELECT k, a_v FROM t_fill_alias_dist ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_v AS a_v + 1);

-- A column the query does not select is still nameable by INTERPOLATE, so it has to stay in the stream
-- even though it is not part of the projection.
SELECT 'interpolating by a column that is not selected';
SELECT k, a_v FROM t_fill_alias_dist ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (a_v AS a_v + w);

-- The alias is not selected at all here, so its body is inlined into the sort expression rather than the
-- projection. The intermediates then belong to the ORDER BY dag, which leaks the same way.
SELECT 'the alias appears only in ORDER BY, parallel replicas';
SELECT k, v FROM t_fill_alias ORDER BY a_v WITH FILL FROM 0 TO 20 STEP 5 INTERPOLATE (v AS v)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

SELECT 'the alias appears only in ORDER BY, Distributed';
SELECT k, v FROM t_fill_alias_dist ORDER BY a_v WITH FILL FROM 0 TO 20 STEP 5 INTERPOLATE (v AS v);

-- The reconciliation failure used to mask this error, which is about the query rather than the transport.
SELECT 'an ORDER BY column as the INTERPOLATE target is still rejected';
SELECT k, a_v FROM t_fill_alias ORDER BY k WITH FILL FROM 0 TO 10 STEP 2 INTERPOLATE (k AS k)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    automatic_parallel_replicas_mode = 0; -- { serverError INVALID_WITH_FILL_EXPRESSION }

DROP TABLE t_fill_alias_dist;
DROP TABLE t_fill_alias;
