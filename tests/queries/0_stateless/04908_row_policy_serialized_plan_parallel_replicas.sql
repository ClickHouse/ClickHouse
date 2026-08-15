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

-- The arms above hold whenever the policy is applied, however the read reached the replicas, so they
-- would still pass if no remote replica ever received a plan. A replica logs this message only where
-- it built a runnable plan out of a received one, so its presence means a remote replica, not the
-- initiator, ran a deserialized plan.
-- A replica runs against its own default database, so its rows are reached through the initiator's
-- row, which does carry this database.
SELECT 'route', max(y) FROM pr_rp SETTINGS log_comment = '04908_pr_route';
SYSTEM FLUSH LOGS query_log, text_log;
SELECT 'remote replicas got the plan', count() > 0 FROM system.text_log
WHERE event_date >= yesterday() AND logger_name = 'TCPHandler' AND message = 'Received query plan'
  AND query_id IN
  (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND NOT is_initial_query AND initial_query_id IN
      (
          SELECT query_id FROM system.query_log
          WHERE current_database = currentDatabase() AND log_comment = '04908_pr_route'
            AND type = 'QueryFinish' AND is_initial_query
      )
  )
SETTINGS max_rows_to_read = 0;

DROP ROW POLICY pr_rp_policy ON pr_rp;
SELECT 'no policy', count() FROM pr_rp SETTINGS optimize_trivial_count_query = 0;

DROP TABLE pr_rp;
