-- Tags: no-azure-blob-storage

CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32, c_nationkey_copy Int32) ENGINE MergeTree ORDER BY c_custkey SETTINGS index_granularity=10;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY'),(100,'UNKNOWN');

INSERT INTO customer SELECT number, 5, 5 FROM numbers(10000);
INSERT INTO customer SELECT number, 6, 6 FROM numbers(10000);
INSERT INTO customer SELECT number, 7, 7 FROM numbers(10000);
INSERT INTO customer SELECT number, 100, 100 FROM numbers(10);


SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';

-- Query with ANTI JOIN that has a predicate left_col==right_col and a predicate that is not left_col==right_col
SELECT count()
FROM customer LEFT ANTI JOIN nation
ON c_nationkey = n_nationkey AND n_name = 'FRANCE'
SETTINGS enable_join_runtime_filters=0;

SELECT count()
FROM customer LEFT ANTI JOIN (SELECT * FROM nation WHERE n_name = 'FRANCE') AS n
ON c_nationkey = n.n_nationkey
SETTINGS enable_join_runtime_filters=0;


SELECT count()
FROM customer LEFT ANTI JOIN nation
ON c_nationkey = n_nationkey AND n_name = 'FRANCE'
SETTINGS enable_join_runtime_filters=1;

SELECT count()
FROM customer LEFT ANTI JOIN (SELECT * FROM nation WHERE n_name = 'FRANCE') AS n
ON c_nationkey = n.n_nationkey
SETTINGS enable_join_runtime_filters=1;
