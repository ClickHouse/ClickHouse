-- Tags: shard

-- { echo }

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY g, number LIMIT -3 BY g;
SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY number LIMIT -3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY g, number LIMIT -3 OFFSET -2 BY g;
SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY number LIMIT -3 OFFSET -2 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY g, number LIMIT -2 OFFSET 3 BY g;
SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY number LIMIT -2 OFFSET 3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY g, number LIMIT 2 OFFSET -3 BY g;
SELECT number, number % 3 AS g FROM remote('127.0.0.1', numbers(15)) ORDER BY number LIMIT 2 OFFSET -3 BY g;

SELECT number, number % 2 AS g1, number % 3 AS g2 FROM remote('127.0.0.1', numbers(12)) ORDER BY g1, g2, number LIMIT -1 BY g1, g2;
SELECT number, number % 2 AS g1, number % 3 AS g2 FROM remote('127.0.0.1', numbers(12)) ORDER BY number LIMIT -1 BY g1, g2;

SELECT number FROM remote('127.0.0.1', numbers(15)) ORDER BY number % 3, number LIMIT -2 BY number % 3;
SELECT number FROM remote('127.0.0.1', numbers(15)) ORDER BY number LIMIT -2 BY number % 3;
