-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel-replicas: the expected counts depend on the read topology, which
-- the ParallelReplicas job overrides globally.

-- Runtime `rows_before_limit_at_least` coverage for distributed TopK planning
-- with prefer_localhost_replica (follow-up to #110136).
--
-- The regression test 04509_preferred_local_preliminary_limit asserts the
-- statistic at runtime only under the default (analyzer) engine, and checks
-- the parallel-replicas path via EXPLAIN plan shape only. Per the review
-- comments on #110136, this adds the two missing RUNTIME assertions:
--   1. the legacy (enable_analyzer = 0) Distributed path, and
--   2. the parallel-replicas path, asserting rows_before_limit_at_least
--      directly rather than the plan shape.
--
-- What would break if the fix regressed: the preliminary LIMIT pushed to the
-- in-process shard would drop source rows from the accounting, so
-- rows_before_limit_at_least would come back smaller than the true qualifying
-- count (60 across two shards / 30 for a single shard with three replicas).

SET distributed_push_down_limit = 1;
SET exact_rows_before_limit = 1;
SET output_format_write_statistics = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_resultset_format = '${data}{"rows_before_limit_at_least":${rows_before_limit:Raw}}\n';

DROP TABLE IF EXISTS preferred_local_prelimit_rt;
CREATE TABLE preferred_local_prelimit_rt
(
    id UInt64,
    sort_key UInt64,
    payload String,
    extra String
)
ENGINE = MergeTree
ORDER BY id;

-- Enough rows to exercise more than one read block.
INSERT INTO preferred_local_prelimit_rt
SELECT
    number,
    cityHash64(number),
    concat('payload-', toString(number)),
    concat('extra-', toString(number))
FROM numbers(20000);

-- (1) Legacy (non-analyzer) Distributed path. 30 qualifying rows per shard
-- across two shards => exact count 60. Must hold with prefer_localhost_replica
-- both off and on.
SET enable_analyzer = 0;
SET prefer_localhost_replica = 0;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit_rt)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template;
SET prefer_localhost_replica = 1;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit_rt)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template;

-- Analyzer parity: the same query on the analyzer engine must report the
-- same 60 (guards against the two paths diverging).
SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit_rt)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template;
SET prefer_localhost_replica = 1;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit_rt)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template;

-- (2) Parallel replicas. A single shard read cooperatively by three replicas
-- => exact count 30. Assert the statistic at runtime, not just the plan shape
-- (which is all 04509 checks for this path).
--
-- `parallel_replicas_local_plan = 0` is deliberate, and this assertion does NOT
-- cover the local-plan path: with the local plan the statistic comes back as 0,
-- because the local branch's preliminary LIMIT is never marked as a shard limit
-- (the marking added in #110136 is gated on a processing stage that parallel
-- replicas never use), so `initRowsBeforeLimit` stops at it and only the remote
-- replicas are counted. Restore this to 1 once #113279 is fixed.
SELECT '' FROM preferred_local_prelimit_rt
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3
FORMAT Template
SETTINGS
    enable_analyzer = 1,
    prefer_localhost_replica = 1,
    allow_experimental_parallel_reading_from_replicas = 0,
    automatic_parallel_replicas_mode = 0,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0;

DROP TABLE preferred_local_prelimit_rt;
