-- Tags: no-parallel, no-ordinary-database
-- no-parallel: SYSTEM ENABLE FAILPOINT is server-global and the failpoint is one-shot, so a concurrent query would consume it
-- no-ordinary-database: the test uses transactions

SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS transactional_count_duplicate;

CREATE TABLE transactional_count_duplicate (n Int64)
    ENGINE = MergeTree ORDER BY n
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO transactional_count_duplicate VALUES (1);
INSERT INTO transactional_count_duplicate VALUES (2);
INSERT INTO transactional_count_duplicate VALUES (3);
INSERT INTO transactional_count_duplicate VALUES (4);

SELECT 'baseline outside a transaction, 4 rows in 4 parts';
SELECT count() FROM transactional_count_duplicate;

-- Inside a transaction the trivial count optimization is declined on the initiator, but a parallel
-- replicas follower has no transaction of its own, so without a guard it answers `count()` from
-- `totalRows()` and its result is merged into the initiator's - `MergingAggregatedStep` just sums the
-- partial states, nothing deduplicates them.
--
-- The failpoint is what makes that deterministic. A follower that takes the shortcut never announces to
-- the coordinator, so it never enters `replicas_used`, and once the initiator's local plan has claimed
-- every mark range the coordinator cancels all unused replicas (`ReadFromRemote.cpp`,
-- `setReadCompletedCallback`) - which can discard the follower's answer before it is consumed and hide the
-- duplication behind a race. `parallel_replicas_wait_for_unused_replicas` skips registering that
-- cancellation callback. It is one-shot (`FIU_ONETIME`), so it has to be re-enabled per query.
-- `throw_on_unsupported_query_inside_transaction = 0` is a no-op for `MergeTree`, which supports
-- transactions. It is needed only for the replicated-database test runs, where the engine is substituted
-- with `ReplicatedMergeTree`: that engine does not support transactions, so a read-only `SELECT` inside one
-- is allowed only with this setting off.
SELECT 'inside a transaction the count must stay 4, not be multiplied by the number of replicas';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
BEGIN TRANSACTION;
SELECT count() FROM transactional_count_duplicate SETTINGS throw_on_unsupported_query_inside_transaction = 0;
COMMIT;

SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

