CREATE TABLE IF NOT EXISTS parallel_replicas_final (x String) ENGINE=ReplacingMergeTree() ORDER BY x;

INSERT INTO parallel_replicas_final SELECT toString(number) FROM numbers(10);

SET max_parallel_replicas=3, enable_parallel_replicas=1, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SELECT * FROM parallel_replicas_final FINAL FORMAT Null;

SET enable_parallel_replicas=2;

SELECT * FROM parallel_replicas_final FINAL FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE IF EXISTS parallel_replicas_final;
