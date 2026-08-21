DROP TABLE IF EXISTS mv2;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS n1_n2_join;
DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
DROP TABLE IF EXISTS n3;

CREATE TABLE n1 (key UInt64, value String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;

CREATE TABLE n2 (key UInt64, value Int64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;

CREATE TABLE n1_n2_join (k UInt64, v1 String, v2 Int64) ENGINE = MergeTree ORDER BY k;

-- mv with explicit target table
CREATE MATERIALIZED VIEW mv TO n1_n2_join
AS SELECT n1.key as k, n1.value as v1, n2.value as v2 from n1 JOIN n2 ON n1.key = n2.key ORDER BY n1.key;

INSERT INTO n2 SELECT number, -number FROM numbers(10);
INSERT INTO n1 SELECT number as key, toString(key) FROM numbers(10);

CREATE TABLE n3 (key UInt64, value String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;
INSERT INTO n3 SELECT number, toString(number + 100) FROM numbers(10);

SET enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;
(SELECT * FROM mv JOIN n3 ON mv.k = n3.key ORDER BY mv.k, n3.key)
EXCEPT
(SELECT * FROM mv JOIN n3 ON mv.k = n3.key ORDER BY mv.k, n3.key settings enable_parallel_replicas=0);

-- materailzed view with inner table
CREATE MATERIALIZED VIEW mv2
AS SELECT n1.key as k, n1.value as v1, n2.value as v2 from n1 JOIN n2 ON n1.key = n2.key ORDER BY n1.key;

INSERT INTO n2 SELECT number, -number FROM numbers(10);
INSERT INTO n1 SELECT number as key, toString(key) FROM numbers(10);

(SELECT * FROM mv2 JOIN n3 ON mv2.k = n3.key ORDER BY mv2.k, n3.key)
EXCEPT
(SELECT * FROM mv2 JOIN n3 ON mv2.k = n3.key ORDER BY mv2.k, n3.key settings enable_parallel_replicas=0);

DROP TABLE mv2;
DROP TABLE mv;
DROP TABLE n1_n2_join;
DROP TABLE n1;
DROP TABLE n2;
DROP TABLE n3;
