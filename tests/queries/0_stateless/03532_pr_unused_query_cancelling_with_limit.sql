-- the test just execute simple select with limit with PR 
-- to test concurrent query cancellation (1) because of limit and (2) because of reading assignment is completed by PR coordinator
DROP TABLE IF EXISTS pr_tt;
CREATE TABLE pr_tt (k UInt64, v String, blob String) ENGINE=MergeTree() ORDER BY k;
INSERT INTO pr_tt SELECT number, toString(number), repeat('blob_', number % 10) FROM numbers(1_000);

SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT * FROM pr_tt LIMIT 10 format Null;

DROP TABLE pr_tt;
