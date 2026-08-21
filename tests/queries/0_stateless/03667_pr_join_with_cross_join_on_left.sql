DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
DROP TABLE IF EXISTS n3;

CREATE TABLE n1 (number UInt64) ENGINE = MergeTree ORDER BY number SETTINGS index_granularity=1;
INSERT INTO n1 SELECT number FROM numbers(3);

CREATE TABLE n2 (number UInt64) ENGINE = MergeTree ORDER BY number SETTINGS index_granularity=1;
INSERT INTO n2 SELECT number FROM numbers(2);

CREATE TABLE n3 (number UInt64) ENGINE = MergeTree ORDER BY number SETTINGS index_granularity=1;
INSERT INTO n3 SELECT number FROM numbers(2);

SET enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;
SELECT * FROM n1, n2 JOIN n3 ON n1.number = n3.number ORDER BY n1.number, n2.number, n3.number;

DROP TABLE n3;
DROP TABLE n2;
DROP TABLE n1;
