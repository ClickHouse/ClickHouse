-- Tags: long, no-parallel-replicas

SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
-- A remote replica receives the serialized plan only when the initiator builds a local plan,
-- and a non-zero automatic mode turns parallel replicas off entirely.
SET parallel_replicas_local_plan = 1, automatic_parallel_replicas_mode = 0;
-- Remote replicas must actually get read tasks: only then do they receive the serialized plan.
SET parallel_replicas_mark_segment_size = 1, merge_tree_min_rows_for_concurrent_read = 1;

DROP TABLE IF EXISTS pr_rp;

CREATE TABLE pr_rp (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0;
INSERT INTO pr_rp SELECT number, number FROM numbers(2000000);

DROP ROW POLICY IF EXISTS pr_rp_policy ON pr_rp;
CREATE ROW POLICY pr_rp_policy ON pr_rp FOR SELECT USING y < 1000000 TO ALL;

-- Each of these read more than the policy allows on the replicas that got the plan.
SELECT 'sum(x)', sum(x) FROM pr_rp;
SELECT 'count()', count() FROM pr_rp SETTINGS optimize_trivial_count_query = 0;
SELECT 'max(y)', max(y) FROM pr_rp;

-- Whether a remote replica really received the plan, rather than the read silently falling back to
-- query text, is asserted in 04909_row_policy_serialized_plan_route.sh: it needs a retry loop around
-- the replica's log row, which this file cannot express.

DROP ROW POLICY pr_rp_policy ON pr_rp;
SELECT 'no policy', count() FROM pr_rp SETTINGS optimize_trivial_count_query = 0;

DROP TABLE pr_rp;
