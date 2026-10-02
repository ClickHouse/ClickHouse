-- Tags: zookeeper, no-parallel, no-shared-merge-tree
-- Regression test for CHECK TABLE on a ReplicatedMergeTree table silently reporting a healthy
-- part as broken (returning 0) when a transient/retryable ZooKeeper error is hit during the check.
-- A retryable error (e.g. a connection loss) must surface as a query error, not a "broken" result.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/63002
-- The `check_table_inject_retryable_zk_error` failpoint is server-global, hence no-parallel.
-- The failpoint lives only in `StorageReplicatedMergeTree::checkDataNext`, so the test must not run
-- under `--replace-replicated-with-shared` (SharedMergeTree has its own check path), hence
-- no-shared-merge-tree.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_check_retryable_zk SYNC;

CREATE TABLE t_check_retryable_zk (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_check_retryable_zk', 'r1')
ORDER BY a;

INSERT INTO t_check_retryable_zk VALUES (1);

-- The part is healthy: CHECK TABLE reports it as passed.
CHECK TABLE t_check_retryable_zk;

-- Inject a transient ZooKeeper hardware error during the check of the (still healthy) part.
SYSTEM ENABLE FAILPOINT check_table_inject_retryable_zk_error;

-- Before the fix this returned a spurious 0 (part reported broken); now it must throw a retryable error.
CHECK TABLE t_check_retryable_zk; -- { serverError KEEPER_EXCEPTION }

-- The failpoint is ONCE, so it is already consumed; the healthy part checks out again.
CHECK TABLE t_check_retryable_zk;

DROP TABLE t_check_retryable_zk SYNC;
