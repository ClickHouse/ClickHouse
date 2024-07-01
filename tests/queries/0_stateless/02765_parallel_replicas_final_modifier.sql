CREATE TABLE IF NOT EXISTS parallel_replicas_final (x String) ENGINE=ReplacingMergeTree() ORDER BY x;

INSERT INTO parallel_replicas_final SELECT toString(number) FROM numbers(10);

SET max_parallel_replicas=3, allow_experimental_parallel_reading_from_replicas=1, use_hedged_requests=0, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT * FROM parallel_replicas_final FINAL FORMAT Null;

SET allow_experimental_parallel_reading_from_replicas=2;

SELECT * FROM parallel_replicas_final FINAL FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE IF EXISTS parallel_replicas_final;
