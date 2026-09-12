-- `findTopNodeOfReplicasPlan` peels the wrappers between the `Union` and the node the two plans have in
-- common, and they do not come one of each kind. An `IN (subquery)` puts a `DelayedCreatingSets` in the
-- middle of that chain: the replica-side branch here is `Expression` over `DelayedCreatingSets` over
-- `Expression` over the reading step, so the loop has to walk all three and stop on the last wrapper
-- above the read.
--
-- Stopping early is not a near miss. `DelayedCreatingSets` does not support dataflow statistics
-- collection, so a search stranded on it matches nothing in the single-replica plan and the query records
-- no statistics at all - the zero-output path the check below rules out. Landing on the read instead is
-- just as bad in the other direction: the reading step records input bytes only, so the output would come
-- back as zero and the cost model would price shipping everything to the initiator at nothing.
--
-- Zero against non-zero is deliberately all this asserts. The recorded figures are a compressed read on
-- one side and a serialized estimate on the other, and the codec moves those two apart, so any ratio
-- between them turns into a check that holds on one build and not another.

DROP TABLE IF EXISTS t_autopr_chain;
DROP TABLE IF EXISTS t_autopr_chain_set;

CREATE TABLE t_autopr_chain(key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_autopr_chain_set(k UInt64) ENGINE = MergeTree ORDER BY k;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3,
    cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET automatic_parallel_replicas_min_bytes_per_replica=0;

INSERT INTO t_autopr_chain SELECT number, number * 2 FROM numbers(1e6);
INSERT INTO t_autopr_chain_set SELECT number FROM numbers(1000);

-- Expression merging would fold the two expressions of the chain together and shorten it, so pin it off
-- to keep the set step surrounded on both sides.
SET query_plan_merge_expressions = 0;

SELECT key IN (SELECT k FROM t_autopr_chain_set) AS flag, value
FROM t_autopr_chain
FORMAT Null SETTINGS log_comment='05137_autopr_set_in_chain';

SET query_plan_merge_expressions = 1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
    (ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0)
        AND (ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0) AS instrumented_above_the_read
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '05137_autopr_set_in_chain') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_chain;
DROP TABLE t_autopr_chain_set;
