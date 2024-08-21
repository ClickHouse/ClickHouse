-- Tags: distributed

DROP TABLE IF EXISTS d_numbers;
CREATE TABLE d_numbers (number UInt32) ENGINE = Distributed(test_cluster_two_shards, system, numbers, rand());

SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2;

SET distributed_product_mode = 'local';

SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2;

DROP TABLE d_numbers;
