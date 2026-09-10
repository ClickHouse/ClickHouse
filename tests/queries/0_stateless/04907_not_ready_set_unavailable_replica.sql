-- Regression test for the logical error "Not-ready Set is passed as the second argument for
-- function 'in'".
--
-- `FutureSetFromSubquery::buildOrderedSetInplace` speculatively builds the set of an `IN` subquery
-- during index analysis. A subquery whose source plan cannot be cloned - a read from a remote
-- cluster is one - takes the destructive path there, which consumes the source plan. When that
-- build throws and the caller swallows the error, nothing can build the set afterwards: statistics
-- based part pruning is such a caller, it keeps reading the part whose condition it failed to
-- evaluate. `FunctionIn` then reported a logical error once the main pipeline reached the
-- condition. The build failure is remembered now and reported as itself instead.

DROP TABLE IF EXISTS t_not_ready_set_unavailable;

CREATE TABLE t_not_ready_set_unavailable (key UInt32, a UInt32)
ENGINE = MergeTree ORDER BY key
SETTINGS auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_not_ready_set_unavailable VALUES (1, 10), (2, 15), (3, 20);

SET use_statistics = 1, use_statistics_for_part_pruning = 1, use_index_for_in_with_subqueries = 1;

-- `a` is not part of the primary key, but it carries automatic statistics, so statistics based part
-- pruning evaluates the `IN` condition and builds the set in place. One replica of the cluster is
-- unreachable, so that build fails.
SELECT count() FROM t_not_ready_set_unavailable
WHERE a IN (SELECT a FROM clusterAllReplicas('test_cluster_1_shard_3_replicas_1_unavailable', currentDatabase(), t_not_ready_set_unavailable)); -- { serverError ALL_CONNECTION_TRIES_FAILED }

DROP TABLE t_not_ready_set_unavailable;
