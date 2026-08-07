-- Tags: no-darwin
-- no-darwin: the downgraded arms below perform a real STREAM read, which is unverified on macOS
--            (mirrors 04658_streaming_queries_pipeline_teardown_use_after_free).

-- A table expression carrying STREAM must never reach a parallel-replicas plan. The engine
-- refused the combination only on the follower, so the initiator built the plan anyway; with a
-- materialized CTE used as an IN set, the resulting plan read the CTE's StorageMemory with no
-- DelayedPortsProcessor gate and the server aborted with a LOGICAL_ERROR. Found by the AST fuzzer.
-- https://github.com/ClickHouse/ClickHouse/pull/110144#issuecomment-5198532644

-- Every arm pins automatic_parallel_replicas_mode = 0: the test runner randomizes it to 2, and at 2
-- Context::canUseTaskBasedParallelReplicas is false, so the guard below is never reached.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_stream_pr;
CREATE TABLE t_stream_pr (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_stream_pr SELECT number FROM numbers(100);

-- Control: the same duplicated materialized-CTE set without STREAM keeps using parallel replicas.
SELECT 'no stream';
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM t_stream_pr
PREWHERE (x IN (t)) OR (x IN (t))
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- allow_experimental_parallel_reading_from_replicas = 2 rejects the query, as it does for FINAL.
SELECT 'stream, throw';
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}}
PREWHERE (x IN (t)) OR (x IN (t))
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- A single reference is rejected the same way: the trigger is STREAM, not the duplication.
SELECT 'stream, throw, single reference';
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}}
PREWHERE x IN (t)
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- allow_experimental_parallel_reading_from_replicas = 1 silently runs without parallel replicas.
-- The STREAM read is then a real one, so it needs a bound to terminate.
SELECT 'stream, downgrade';
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}}
    PREWHERE (x IN (t)) OR (x IN (t)) LIMIT 5)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- The guard clears the setting rather than skipping one plan branch, so the plan-based path,
-- which aborted with `Cannot serialize FutureSetFromSubquery with no query plan`, is covered too.
SELECT 'stream, downgrade, plan based';
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}}
    PREWHERE (x IN (t)) OR (x IN (t)) LIMIT 5)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 1;

-- The custom-key and sampling-key modes distribute the read through different code, none of which
-- gates STREAM: before the fix these hung instead of returning. Every row they match fits under the
-- LIMIT, so a downgraded arm returns them all and the bound is never what ends the query.
SELECT 'stream, custom key sampling, throw';
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'x', max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'stream, custom key sampling, downgrade';
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'x', max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

SELECT 'stream, custom key range, throw';
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_range',
    parallel_replicas_custom_key = 'x', parallel_replicas_custom_key_range_lower = 0,
    parallel_replicas_custom_key_range_upper = 10, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'stream, custom key range, downgrade';
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_mode = 'custom_key_range',
    parallel_replicas_custom_key = 'x', parallel_replicas_custom_key_range_lower = 0,
    parallel_replicas_custom_key_range_upper = 10, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- The sampling-key mode splits by the sampling expression, so it needs a table that supports sampling.
DROP TABLE IF EXISTS t_stream_samp;
CREATE TABLE t_stream_samp (x UInt64, h UInt64) ENGINE = MergeTree ORDER BY (h, x) SAMPLE BY h;
INSERT INTO t_stream_samp SELECT number, sipHash64(number) FROM numbers(1000);

SELECT 'stream, sampling key, throw';
SELECT count() FROM (
    SELECT x FROM t_stream_samp STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 2, parallel_replicas_mode = 'sampling_key', max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- The offset split is gated on parallel_replicas_count > 1, which the initiator sets for each
-- follower it dispatches to; a single-node stateless test has no follower, so this arm sets it
-- directly (the parallel_replicas_count > 1 term of sampling.use_sampling in
-- MergeTreeDataSelectExecutor). No user-facing setting reaches the split here.
SELECT 'stream, sampling key, downgrade';
SELECT count() FROM (
    SELECT x FROM t_stream_samp STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_mode = 'sampling_key', max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_count = 2,
    parallel_replica_offset = 0, automatic_parallel_replicas_mode = 0;

-- STREAM reaches the same storage through a table function, and the guard reads the modifiers of both
-- carriers. timeSeriesSamples resolves to the TimeSeries samples inner table, a plain MergeTree, so it
-- reports supportsStreaming() and passes the analyzer's ILLEGAL_STREAM check like a named table does.
SET allow_experimental_time_series_table = 1;
DROP TABLE IF EXISTS t_stream_ts;
CREATE TABLE t_stream_ts ENGINE = TimeSeries;
INSERT INTO t_stream_ts (metric_name, tags, time_series)
    VALUES ('m', map('a', 'b'), [(now64(3), 1.0), (now64(3), 2.0), (now64(3), 3.0)]);

SELECT 'stream, table function, custom key sampling, throw';
SELECT count() FROM (
    SELECT value FROM timeSeriesSamples(t_stream_ts) STREAM CURSOR {'1': {'block_number': 0}}
    PREWHERE value < 10 LIMIT 3)
SETTINGS enable_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'value', max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'stream, table function, custom key sampling, downgrade';
SELECT count() FROM (
    SELECT value FROM timeSeriesSamples(t_stream_ts) STREAM CURSOR {'1': {'block_number': 0}}
    PREWHERE value < 10 LIMIT 3)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'value', max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- Control: the guard is keyed on a mode being able to distribute the read, not on the setting alone.
-- With one replica no mode distributes, so STREAM stays allowed.
SELECT 'stream, one replica, allowed';
SELECT count() FROM (
    SELECT x FROM t_stream_pr STREAM CURSOR {'1': {'block_number': 0}} PREWHERE x < 10 LIMIT 10)
SETTINGS enable_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'x', max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- Control: FINAL under a custom-key mode already returns the expected result, so it is left alone.
-- Each replica reports its own partial count, so sum them to compare against the deduplicated table.
DROP TABLE IF EXISTS t_stream_final;
CREATE TABLE t_stream_final (x UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY x;
INSERT INTO t_stream_final SELECT number, 1 FROM numbers(100);
INSERT INTO t_stream_final SELECT number, 2 FROM numbers(100);

-- This is the only arm that actually dispatches custom-key parallel replicas, and that entry point
-- refuses serialize_query_plan ("Parallel replicas with custom key are not supported with
-- serialize_query_plan enabled", ClusterProxy/executeQuery.cpp:1111), which the CI `distributed plan`
-- flavour enables in the default profile. The STREAM arms are refused or downgraded before dispatch.
SET serialize_query_plan = 0;

SELECT 'final, custom key sampling, allowed';
SELECT sum(c) FROM (
    SELECT count() AS c FROM t_stream_final FINAL
    SETTINGS enable_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling',
        parallel_replicas_custom_key = 'x', max_parallel_replicas = 2,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
        parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0);

DROP TABLE t_stream_final;
DROP TABLE t_stream_ts;
DROP TABLE t_stream_samp;
DROP TABLE t_stream_pr;
