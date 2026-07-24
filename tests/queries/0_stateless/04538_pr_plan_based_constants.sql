-- Plan-based parallel replicas ships a serialized plan fragment with query-level constants folded on
-- the initiator, so every replica uses the same value. This guards against per-replica constant
-- divergence (e.g. randConstant(), now64()), which the old ReadFromParallelRemoteReplicasStep path had
-- to reconcile with addConvertingActions. count(DISTINCT <constant>) must stay 1 across replicas.

DROP TABLE IF EXISTS t_pr_const;

CREATE TABLE t_pr_const (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_const SELECT number FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;

-- Slow the initiator's local read so the remote replicas actually produce rows; if the constant were
-- evaluated per replica each would surface its own value and the DISTINCT count would exceed 1.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- randConstant(): one random value per query -- must be identical on every replica.
SELECT count(DISTINCT c) FROM (SELECT a, randConstant() AS c FROM t_pr_const);
-- now64(): evaluated once per query -- likewise must not diverge per replica.
SELECT count(DISTINCT c) FROM (SELECT a, now64() AS c FROM t_pr_const);

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_pr_const;
