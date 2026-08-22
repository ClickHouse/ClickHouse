-- Tags: distributed

SELECT *
FROM
(
    SELECT *
    FROM system.numbers
    WHERE number = 100
    UNION ALL
    SELECT *
    FROM system.numbers
    WHERE number = 100
)
LIMIT 2
SETTINGS max_threads = 1 FORMAT Null;

DROP TABLE IF EXISTS d_numbers;
CREATE TABLE d_numbers (number UInt32) ENGINE = Distributed(test_cluster_two_shards, system, numbers, rand());

SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2;
SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2 SETTINGS max_threads = 1, prefer_localhost_replica=1;
SELECT sum(number) FROM (select * from remote('127.0.0.{1,1,1}', system.numbers) AS n WHERE n.number = 100 LIMIT 3) SETTINGS max_threads = 2, prefer_localhost_replica=1;

SET distributed_product_mode = 'local';

SELECT '100' AS number FROM d_numbers AS n WHERE n.number = 100 LIMIT 2;

DROP TABLE d_numbers;
