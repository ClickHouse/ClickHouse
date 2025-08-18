CREATE TABLE nation(n_nationkey Int32, n_name String) ENGINE MergeTree ORDER BY n_nationkey;
CREATE TABLE customer(c_custkey Int32, c_nationkey Int32) ENGINE MergeTree ORDER BY c_custkey;
CREATE TABLE orders(o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2)) ENGINE MergeTree ORDER BY o_orderkey;

INSERT INTO nation VALUES (5,'ETHIOPIA'),(6,'FRANCE'),(7,'GERMANY');

INSERT INTO customer SELECT number, 5 FROM numbers(500);
INSERT INTO customer SELECT number, 6 FROM numbers(6000);
INSERT INTO customer SELECT number, 7 FROM numbers(70000);

INSERT INTO orders SELECT number, number/10000, number%1000 FROM numbers(1000000);

SET enable_join_runtime_filters=1;

SELECT avg(o_totalprice)
FROM orders, customer, nation
WHERE c_custkey = o_custkey AND c_nationkey = n_nationkey AND n_name = 'FRANCE';


SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey+1 AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey+1 AND n_name = 'FRANCE';

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey+1 AND n_name = 'FRANCE'
SETTINGS join_runtime_bloom_filter_bytes = 100500000; -- {serverError PARAMETER_OUT_OF_BOUND}

SELECT count()
FROM customer, nation
WHERE c_nationkey+1 = n_nationkey+1 AND n_name = 'FRANCE'
SETTINGS join_runtime_bloom_filter_hash_functions = 20; -- {serverError PARAMETER_OUT_OF_BOUND}

SELECT count()
FROM customer, nation
WHERE c_nationkey = n_nationkey AND n_name = 'FRANCE'
SETTINGS join_runtime_bloom_filter_bytes = 4096, join_runtime_bloom_filter_hash_functions = 2;
