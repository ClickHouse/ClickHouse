-- Tags: shard

-- { echo }

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

-- Negative limit
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number LIMIT -3;

-- Negative limit with ties
SELECT intDiv(number, 3) AS x FROM remote('127.0.0.1', numbers(12)) ORDER BY x LIMIT -4 WITH TIES;

-- Negative limit + negative offset
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number LIMIT -3, -4;

-- Negative limit + negative offset with ties
SELECT intDiv(number, 3) AS x FROM remote('127.0.0.1', numbers(12)) ORDER BY x LIMIT -3, -5 WITH TIES;

-- Negative limit + positive offset
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number LIMIT 3, -4;

-- Negative limit + positive offset with ties
SELECT intDiv(number, 3) AS x FROM remote('127.0.0.1', numbers(12)) ORDER BY x LIMIT 3, -4 WITH TIES;

-- Positive limit + negative offset
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number LIMIT -3, 4;

-- Positive limit + negative offset with ties
SELECT intDiv(number, 3) AS x FROM remote('127.0.0.1', numbers(12)) ORDER BY x LIMIT -3, 4 WITH TIES;

-- Fractional limit
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number LIMIT 0.5;

-- Fractional limit with offset
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number LIMIT 0.3 OFFSET 0.2;

-- Fractional limit with ties
SELECT intDiv(number, 3) AS x FROM remote('127.0.0.1', numbers(12)) ORDER BY x LIMIT 0.5 WITH TIES;

-- Fractional limit with offset and ties
SELECT intDiv(number, 3) AS x FROM remote('127.0.0.1', numbers(12)) ORDER BY x LIMIT 0.3 OFFSET 0.2 WITH TIES;
