-- Tags: no-parallel, no-ordinary-database, no-old-analyzer
-- no-parallel: SYSTEM ENABLE FAILPOINT is server-global and the failpoint is one-shot, so a concurrent query would consume it
-- no-ordinary-database: the test uses transactions
-- no-old-analyzer: `optimize_trivial_count_with_sparsity_filter` is implemented only in the analyzer

-- `applyTrivialCountWithSparsityFilterIfPossible` serves `count()` with a predicate that exactly
-- partitions a column into its defaults and non-defaults from the per-column `num_defaults` counter.
-- Its transaction check precedes the block that disables parallel replicas, so inside a transaction the
-- initiator declines while parallel replicas stays enabled. A follower has no transaction of its own, so
-- it answers from the counter and its result is merged into the initiator's - `MergingAggregatedStep` just
-- sums the partial states, nothing deduplicates them.
--
-- Without the failpoint below this is masked by a race rather than being correct. A follower that takes
-- the shortcut never announces to the coordinator, so it never enters `replicas_used`, and once the
-- initiator's local plan has claimed every mark range the coordinator cancels all unused replicas
-- (`ReadFromRemote.cpp`, `setReadCompletedCallback`) - usually discarding the follower's answer before it
-- is consumed. `parallel_replicas_wait_for_unused_replicas` skips registering that cancellation callback,
-- which makes the duplication deterministic instead of dependent on who wins the race. The failpoint is
-- one-shot (`FIU_ONETIME`), so it has to be re-enabled before every query that needs it.

SET optimize_trivial_count_query = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE IF EXISTS transactional_count_duplicate_sparsity;

CREATE TABLE transactional_count_duplicate_sparsity (n UInt64, s UInt32)
    ENGINE = MergeTree ORDER BY n
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1,
             ratio_of_defaults_for_sparse_serialization = 0.9,
             compute_exact_num_defaults_for_sparse_columns = 1;

-- 19000 defaults and 1000 non-defaults per part, so `s` is sparse-encoded with an exact `num_defaults`.
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);
INSERT INTO transactional_count_duplicate_sparsity SELECT number, if(number % 20 = 0, number + 1, 0) FROM numbers(20000);

-- Guard against the test silently becoming vacuous: if the rewrite stops engaging for this table and
-- predicate shape, the queries below would return the right answer for the wrong reason.
SELECT 'the sparsity filter rewrite must engage, otherwise nothing below is meaningful';
SELECT countIf(explain LIKE '%Optimized trivial count with sparsity filter%') > 0
    FROM (EXPLAIN SELECT count() FROM transactional_count_duplicate_sparsity WHERE s = 0);

SELECT 'baseline outside a transaction, defaults of `s` out of 160000 rows';
SELECT count() FROM transactional_count_duplicate_sparsity WHERE s = 0;
SELECT 'baseline outside a transaction, non-defaults of `s`';
SELECT count() FROM transactional_count_duplicate_sparsity WHERE s > 0;

-- `throw_on_unsupported_query_inside_transaction = 0` is a no-op for `MergeTree`, which supports
-- transactions. It matters only where the engine is substituted with `ReplicatedMergeTree`, which does not
-- support transactions and so allows a read-only `SELECT` inside one only with this setting off. That does
-- not happen for this test today - every replicated-database job runs with the old analyzer, and the
-- `no-old-analyzer` tag above skips it there - so this is kept only in case that changes.
SELECT 'inside a transaction the defaults count must stay 152000, not be multiplied by the number of replicas';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
BEGIN TRANSACTION;
SELECT count() FROM transactional_count_duplicate_sparsity WHERE s = 0 SETTINGS throw_on_unsupported_query_inside_transaction = 0;
COMMIT;

SELECT 'inside a transaction the non-defaults count must stay 8000, not be multiplied by the number of replicas';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
BEGIN TRANSACTION;
SELECT count() FROM transactional_count_duplicate_sparsity WHERE s > 0 SETTINGS throw_on_unsupported_query_inside_transaction = 0;
COMMIT;

SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

DROP TABLE transactional_count_duplicate_sparsity;
