-- Regression test for the family of https://github.com/ClickHouse/ClickHouse/issues/111728
-- (closed by https://github.com/ClickHouse/ClickHouse/pull/111919, which did not cover the
-- read_tasks parallel-replicas path).
--
-- ORDER BY ... WITH FILL ... INTERPOLATE used to make the "Before INTERPOLATE" step pin every
-- available chain column as one of its outputs, including the placeholder column of an IN set
-- (type `Set`) or of a lambda (type `Function`). Those two types have no serialization, so on a
-- replica producing a mergeable state the pipeline output header could not be sent over the wire:
-- `Code: 48 ... Serialization is not implemented for data type Set`.
--
-- parallel_replicas_local_plan = 0 forces the read remote, which makes the failure deterministic
-- (30 of 30 trials); at its default of 1 it reproduced in 8 of 100 trials.

DROP TABLE IF EXISTS t_04733;
CREATE TABLE t_04733 (k UInt32, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04733 VALUES (1, '8');
INSERT INTO t_04733 VALUES (2, '8');

DROP TABLE IF EXISTS t_04733_unprojected;
CREATE TABLE t_04733_unprojected (n Float32, source String, inter UInt64, inter2 UInt64)
ENGINE = MergeTree ORDER BY n;
INSERT INTO t_04733_unprojected SELECT toFloat32(number % 10), 'original', number, number + 1
FROM numbers(10) WHERE (number % 3) = 1;

-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this test exercises the placeholder pin, not plan serialization.
SET serialize_query_plan = 0;

-- The fix is in the analyzer planner, so under the old-analyzer job every arm below would pass on
-- unpatched master and the test would be vacuous there.
SET enable_analyzer = 1;

-- automatic_parallel_replicas_mode is randomized to 2 by the test runner, and at a non-zero value
-- buildContext clears enable_parallel_replicas (InterpreterSelectQueryAnalyzer.cpp:137-156). That
-- happens after the per-query SETTINGS are applied, so the SETTINGS clauses below cannot win.
SET automatic_parallel_replicas_mode = 0;

-- Rows are asserted, not merely the absence of an exception: a test checking only that the query
-- runs would still pass if INTERPOLATE silently stopped interpolating. The expected values below
-- come from the same queries with enable_parallel_replicas = 0.

SELECT 'scalar IN';
SELECT k, s FROM t_04733 WHERE s IN ('8')
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'empty INTERPOLATE';
SELECT k, s FROM t_04733 WHERE s IN ('8')
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE ()
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'subquery IN';
SELECT k, s FROM t_04733 WHERE s IN (SELECT '8')
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'NOT IN';
SELECT k, s FROM t_04733 WHERE s NOT IN ('9')
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'IN in the SELECT list';
SELECT k, s IN ('8') AS f FROM t_04733
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (f AS 1)
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Second placeholder type: a lambda's `Function(String -> String)`. It needs no stage boundary at
-- all: `FillingTransform` inserts a default into every header column when it synthesizes a row, and
-- the placeholder cannot accept one, so a plain single-node query fails with
-- `Code: 48 ... Cannot insert into Function`.
SELECT 'lambda, single node';
SELECT k, arrayMap(x -> x || s, ['a']) AS m FROM t_04733
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (m AS ['z'])
SETTINGS enable_parallel_replicas = 0;

SELECT 'lambda';
SELECT k, arrayMap(x -> x || s, ['a']) AS m FROM t_04733
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (m AS ['z'])
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Each of the three replicas reads the same table, so every stored row appears three times.
SELECT 'clusterAllReplicas';
SELECT k, s FROM clusterAllReplicas('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), t_04733)
WHERE s IN ('8')
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Controls. These are correct before the fix and must stay correct: they pin the two ways a
-- narrower or a wider rule would go wrong.

-- A storable column that only an INTERPOLATE expression references, and the SELECT does not
-- project, must stay pinned into the header.
SELECT 'unprojected interpolate source';
SELECT n, source, inter FROM t_04733_unprojected
ORDER BY n WITH FILL FROM 0 TO 4 INTERPOLATE (inter AS inter2 + inter)
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- `Nothing` overrides doGetSerialization and serializes fine, so `Nullable(Nothing)` and
-- `Array(Nothing)` must keep travelling in the header. `z` and `e` below are projected but are
-- neither an ORDER BY key nor an INTERPOLATE target, which is what makes them reach the header
-- through the same code path the fix filters; a rule keyed on `cannotBeStoredInTables()` instead
-- drops them and these two queries fail with `Code: 47`.
SELECT 'Nullable(Nothing)';
SELECT k, s, NULL AS z FROM t_04733 WHERE s = '8'
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'Array(Nothing)';
SELECT k, s, [] AS e FROM t_04733 WHERE s = '8'
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7')
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- The same two types as INTERPOLATE targets, which reach the DAG by a different route (the
-- interpolate expressions are added unconditionally), so they do not discriminate the rule above.
SELECT 'Nullable(Nothing) as interpolate target';
SELECT k, NULL AS z FROM t_04733
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (z AS NULL)
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'Array(Nothing) as interpolate target';
SELECT k, [] AS e FROM t_04733
ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (e AS [])
SETTINGS parallel_replicas_local_plan = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE t_04733;
DROP TABLE t_04733_unprojected;
