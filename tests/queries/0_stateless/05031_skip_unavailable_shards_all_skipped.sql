-- Tags: shard, no-fasttest

-- skip_unavailable_shards returns a result built from the shards that answered. When no shard
-- answered there is no such result, and returning an empty one breaks the guarantee that an
-- aggregate without GROUP BY produces exactly one row.
-- https://github.com/ClickHouse/ClickHouse/issues/115646

DROP TABLE IF EXISTS dist_05031_all_dead;
DROP TABLE IF EXISTS dist_05031_partial;
DROP TABLE IF EXISTS dist_05031_mixed;
DROP TABLE IF EXISTS data_05031;

CREATE TABLE data_05031 (c0 UInt64) ENGINE = MergeTree() PRIMARY KEY tuple();
INSERT INTO data_05031 SELECT number FROM numbers(3);

-- Three nodes, every one of them at a dead port.
CREATE TABLE dist_05031_all_dead (c0 UInt64)
ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, currentDatabase(), data_05031);

-- Two shards, the first reachable and the second at a dead port.
CREATE TABLE dist_05031_partial (c0 UInt64)
ENGINE = Distributed(test_unavailable_shard, currentDatabase(), data_05031);

-- The same two shards, pointed at a table that does not exist, so the reachable one is skipped while
-- the plan is built and the dead one while the query runs.
CREATE TABLE dist_05031_mixed (c0 UInt64)
ENGINE = Distributed(test_unavailable_shard, currentDatabase(), absent_05031);

SET prefer_localhost_replica = 0;

-- Every shard skipped: the query fails instead of returning nothing. Asserted for an aggregate
-- without GROUP BY, where the empty result was also a wrong row count, and for a plain read, where
-- 04050 already asserts the same for the local shard path.
SELECT count() FROM dist_05031_all_dead SETTINGS skip_unavailable_shards = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT sum(c0) FROM dist_05031_all_dead SETTINGS skip_unavailable_shards = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT uniqExact(c0) FROM dist_05031_all_dead SETTINGS skip_unavailable_shards = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT * FROM dist_05031_all_dead SETTINGS skip_unavailable_shards = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- The same query wrapped in a subquery, which builds an initiator-side aggregation step and
-- therefore returned one row even while the direct form returned none.
SELECT count() FROM (SELECT c0 FROM dist_05031_all_dead) SETTINGS skip_unavailable_shards = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- One shard answering is enough: these must keep returning the partial result.
SELECT count() FROM dist_05031_partial SETTINGS skip_unavailable_shards = 1;
SELECT count() FROM dist_05031_partial SETTINGS skip_unavailable_shards = 1, distributed_group_by_no_merge = 1;
SELECT sum(c0) FROM dist_05031_partial SETTINGS skip_unavailable_shards = 1;

-- A shard that streamed rows and only then failed has answered too, even though it is the sole shard
-- and is skipped. Counting it as having produced nothing would make the query below raise instead of
-- returning what arrived. `max_block_size = 1` sends blocks as they are produced, so the rows precede
-- the exception.
-- How many of them arrive before it is a race between the shard and the initiator tearing the
-- pipeline down, so only the two properties below hold for every outcome: some row arrived, and the
-- row the shard throws on is not among them.
SELECT count() > 0, max(x) < 999 FROM
(
    SELECT number AS x FROM cluster('test_shard_localhost', numbers(1000))
    WHERE throwIf(number = 999, 'stop here', toInt32(60)) = 0
)
SETTINGS skip_unavailable_shards = 1, max_block_size = 1, allow_custom_error_code_in_throwif = 1;

-- The all-skipped check must not depend on the limits, which are off by default.
SELECT count() FROM dist_05031_all_dead
SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 0, max_skip_unavailable_shards_ratio = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- A local shard skipped while the plan is built and a remote shard skipped while the query runs are
-- reported through different call sites, and neither alone empties this two-shard cluster. Both must
-- count as having produced nothing for the condition to hold, so a shape mixing them is the only one
-- that pins the two together: shard 1 is local with the table missing, shard 2 is at a dead port.
-- Reached through the tracker, not through the uninitialized-plan guard that `04050` covers, since a
-- remote shard is present here and the plan is built.
-- `serialize_query_plan = 0` because a serialized plan is analyzed on the initiator, which resolves
-- the missing table there and raises `UNKNOWN_TABLE` before the local shard is ever skipped.
SELECT * FROM dist_05031_mixed
SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1, serialize_query_plan = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- The limits still apply before everything is skipped, and still count logical shards.
SELECT count() FROM dist_05031_partial
SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.4; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }
SELECT count() FROM dist_05031_partial
SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 0, max_skip_unavailable_shards_ratio = 0.6;

-- The limits are reported as soon as they are crossed, before the topology is known to be complete;
-- only the all-skipped check waits for that. 04050 pins the same for a plan with no remote shard,
-- which never reaches the completion point at all.
SELECT count() FROM dist_05031_all_dead
SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 1; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }

-- Explaining an all-dead cluster stays a diagnostic: the explain paths build their own executors
-- over the same shards, and a skip they report is not a skip the query suffered.
-- Only `distributed = 1` reaches those executors, and each of the two arms that do is able to fail
-- by a different means, because each goes through a different describe path. `EXPLAIN PIPELINE`
-- builds a pipeline, so its executors reach the point where the topology is complete and the
-- all-skipped condition is evaluated. `EXPLAIN PLAN` only formats the plan and never reaches that
-- point, so there only a limit can be crossed: it carries one for that reason. The other three arms
-- describe the initiator plan alone and never open a connection.
-- A serialized plan is described without building it, so neither of the two reaches an executor then:
-- `EXPLAIN PIPELINE distributed = 1` is rejected outright, and `EXPLAIN PLAN distributed = 1` reports
-- no skip. Both therefore pin the setting off, and the last arm pins it on to cover that path.
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT count() FROM dist_05031_all_dead) SETTINGS skip_unavailable_shards = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM dist_05031_all_dead) SETTINGS skip_unavailable_shards = 1;
SELECT count() > 0 FROM (EXPLAIN PLAN distributed = 1 SELECT count() FROM dist_05031_all_dead)
SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 1, serialize_query_plan = 0;
SELECT count() > 0 FROM (EXPLAIN PIPELINE distributed = 1 SELECT count() FROM dist_05031_all_dead)
SETTINGS skip_unavailable_shards = 1, serialize_query_plan = 0;
SELECT count() > 0 FROM (EXPLAIN PLAN distributed = 1 SELECT count() FROM dist_05031_all_dead)
SETTINGS skip_unavailable_shards = 1, serialize_query_plan = 1;

-- The two `distributed = 1` arms above do reach the shards: they report the skips through
-- `DistributedShardsSkipped` while still returning their explain output. Asserting both halves is
-- what distinguishes a diagnostic that tolerates the dead cluster from one that fails on it, since a
-- diagnostic that failed would raise instead and neither its output nor its row would be here.
SYSTEM FLUSH LOGS query_log;
SELECT count() = 2, min(skipped) > 0
FROM
(
    SELECT ProfileEvents['DistributedShardsSkipped'] AS skipped
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND query LIKE '%EXPLAIN % distributed = 1 SELECT count() FROM dist\_05031\_all\_dead%'
      AND query NOT LIKE '%serialize\_query\_plan = 1%'
      AND query NOT LIKE '%system.query\_log%'
);

-- A Distributed table can be the target of another one. When the inner layer has no shard left, the
-- failure it raises is an exception from a shard that is itself reachable, so the outer query
-- propagates it under the default mode. `unavailable_or_exception_before_processing` is the mode that
-- tolerates it, and does so only while the outer shard has produced nothing.
-- Both outer shards read the inner table, so all of them fail and the tolerant mode has no partial
-- result to keep either. 05032 covers the shape where one outer shard is healthy, which needs global
-- databases and so cannot be asserted from a parallel test.
CREATE TABLE dist_05031_inner (c0 UInt64)
ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, currentDatabase(), data_05031);

CREATE TABLE dist_05031_outer (c0 UInt64)
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dist_05031_inner);

SELECT count() FROM dist_05031_outer
SETTINGS skip_unavailable_shards = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT count() FROM dist_05031_outer
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT count() FROM dist_05031_outer
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing'; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- The three arms above report the same code through two different mechanisms, so the code alone does
-- not say which one ran. The number of shards the outer query skipped separates them: under the two
-- strict modes it skips none and reports the inner error as its own, while the tolerant mode ignores
-- that error, skips both outer shards, and reaches the outer all-skipped condition.
SYSTEM FLUSH LOGS query_log;
SELECT countIf(mode = 'strict' AND skipped = 0) = 2, countIf(mode = 'tolerant' AND skipped = 2) = 1
FROM
(
    SELECT
        if(query LIKE '%unavailable\_or\_exception\_before\_processing%', 'tolerant', 'strict') AS mode,
        ProfileEvents['DistributedShardsSkipped'] AS skipped
    FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query
      AND type = 'ExceptionWhileProcessing'
      AND query LIKE '%FROM dist\_05031\_outer%'
);

-- A reachable single shard still finalizes the aggregate itself: no initiator-side Aggregating step.
-- Asserting the plan shape keeps the fix from silently costing that pushdown.
CREATE TABLE dist_05031_reachable (c0 UInt64)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), data_05031);

SELECT count() FROM dist_05031_reachable SETTINGS skip_unavailable_shards = 1;
SELECT count() FROM (EXPLAIN PIPELINE SELECT count() FROM dist_05031_reachable) WHERE explain ILIKE '%Aggregating%';
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM dist_05031_reachable) WHERE explain ILIKE '%ReadFromRemote%';

DROP TABLE dist_05031_reachable;
DROP TABLE dist_05031_all_dead;
DROP TABLE dist_05031_partial;
DROP TABLE dist_05031_mixed;
DROP TABLE data_05031;
