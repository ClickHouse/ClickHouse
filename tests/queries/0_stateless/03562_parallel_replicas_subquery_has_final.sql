SET allow_suspicious_primary_key = 1;

CREATE OR REPLACE TABLE t0 (c0 Int) ENGINE = SummingMergeTree() ORDER BY tuple();
INSERT INTO t0 VALUES (1);

SELECT 1 FROM t0 RIGHT JOIN (SELECT c0 FROM t0 FINAL) tx ON TRUE GROUP BY c0;

SET enable_parallel_replicas = 1, parallel_replicas_only_with_analyzer = 0, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SELECT 1 FROM t0 RIGHT JOIN (SELECT c0 FROM t0 FINAL) tx ON TRUE GROUP BY c0;

DROP TABLE t0;
