-- Tags: no-parallel
-- Tag no-parallel: enables a server-global failpoint that forces Default coordination mode on
-- parallel-replicas followers, which would perturb other concurrent parallel-replicas queries.

-- Regression test: a parallel-replicas follower that plans an in-order read in Default coordination
-- mode (requesting the bare-table stream) while the initiator splits it into `#split_i` streams used
-- to hit `Got read request from replica N for unknown stream db.table` (LOGICAL_ERROR / server abort
-- in debug and sanitizer builds). The coordinator now finishes such a request gracefully, the same way
-- it already drops the follower's announcement under snapshot pinning. The failpoint
-- `parallel_replicas_force_default_mode_on_follower` reproduces the mode divergence on a single-server
-- test cluster (which otherwise plans every replica identically).

DROP TABLE IF EXISTS t_pr_unknown_stream;

CREATE TABLE t_pr_unknown_stream (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

INSERT INTO t_pr_unknown_stream SELECT number, number FROM numbers_mt(1000000);
OPTIMIZE TABLE t_pr_unknown_stream FINAL;

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET max_threads = 4;
SET optimize_read_in_order = 1;

SYSTEM ENABLE FAILPOINT parallel_replicas_force_default_mode_on_follower;

-- Before the fix this aborts the server; after the fix the dropped followers finish gracefully and the
-- initiator's splits cover the whole read, so the results match the known baseline (a = 0..999999).
SELECT count() = 1000000, sum(a) = 499999500000 FROM (SELECT a FROM t_pr_unknown_stream ORDER BY a);
SELECT count() = 1000000, sum(a) = 499999500000 FROM (SELECT a FROM t_pr_unknown_stream ORDER BY a DESC);

SYSTEM DISABLE FAILPOINT parallel_replicas_force_default_mode_on_follower;

DROP TABLE t_pr_unknown_stream;
