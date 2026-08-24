DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS n1_n2_join;
DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;

CREATE TABLE n1 (key UInt64, value String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;

CREATE TABLE n2 (key UInt64, value Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;

CREATE TABLE n1_n2_join (k UInt64, v1 String, v2 Int64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW mv TO n1_n2_join
AS SELECT n1.key as k, n1.value as v1, n2.value as v2 from n1 JOIN n2 ON n1.key = n2.key ORDER BY n1.key;

INSERT INTO n2 SELECT number, -number FROM numbers(10);

SET enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;

-- inserting into n1 (left table) triggers JOIN in the materialized view
INSERT INTO n1 values(0, '11');
INSERT INTO n1 SELECT number, toString(number) FROM numbers(10);

SELECT * FROM n1_n2_join ORDER BY ALL;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS n1_n2_join;
DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
