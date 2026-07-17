-- { echoOn }
Select sum(number + 1) from numbers(10);
Select sum(1 + number) from numbers(10);
Select sum(number - 1) from numbers(10);
Select sum(1 - number) from numbers(10);
EXPLAIN SYNTAX (Select sum(number + 1) from numbers(10));
EXPLAIN SYNTAX (Select sum(1 + number) from numbers(10));
EXPLAIN SYNTAX (Select sum(number - 1) from numbers(10));
EXPLAIN SYNTAX (Select sum(1 - number) from numbers(10));

WITH 1::Nullable(UInt64) as my_literal Select sum(number + my_literal) from numbers(0);
WITH 1::Nullable(UInt64) as my_literal Select sum(number) + my_literal * count() from numbers(0);
EXPLAIN SYNTAX (WITH 1::Nullable(UInt64) as my_literal Select sum(number + my_literal) from numbers(0));
EXPLAIN SYNTAX (WITH 1::Nullable(UInt64) as my_literal Select sum(number) + my_literal * count() from numbers(0));
-- { echoOff }

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    uint64 UInt64,
    float64 Float64,
    decimal32 Decimal32(5),
) ENGINE=MergeTree ORDER BY uint64;

-- Use Float64 numbers divisible by 1/16 (or some other small power of two), so that their sum doesn't depend on summation order.
INSERT INTO test_table VALUES (1, 1.125, 1.11);
INSERT INTO test_table VALUES (2, 2.250, 2.22);
INSERT INTO test_table VALUES (3, 3.375, 3.33);
INSERT INTO test_table VALUES (4, 4.500, 4.44);
INSERT INTO test_table VALUES (5, 5.625, 5.55);

-- { echoOn }
SELECT sum(uint64 + 1 AS i) from test_table where i > 0;
SELECT sum(uint64 + 1) AS j from test_table having j > 0;
SELECT sum(uint64 + 1 AS i) j from test_table where i > 0 having j > 0;
SELECT sum((uint64 AS m) + (1 AS n)) j from test_table where m > 0 and n > 0 having j > 0;
SELECT sum(((uint64 AS m) + (1 AS n)) AS i) j from test_table where m > 0 and n > 0 and i > 0 having j > 0;
EXPLAIN SYNTAX (SELECT sum(uint64 + 1 AS i) from test_table where i > 0);
EXPLAIN SYNTAX (SELECT sum(uint64 + 1) AS j from test_table having j > 0);
EXPLAIN SYNTAX (SELECT sum(uint64 + 1 AS i) j from test_table where i > 0 having j > 0);
EXPLAIN SYNTAX (SELECT sum((uint64 AS m) + (1 AS n)) j from test_table where m > 0 and n > 0 having j > 0);
EXPLAIN SYNTAX (SELECT sum(((uint64 AS m) + (1 AS n)) AS i) j from test_table where m > 0 and n > 0 and i > 0 having j > 0);

SELECT sum(1 + uint64 AS i) from test_table where i > 0;
SELECT sum(1 + uint64) AS j from test_table having j > 0;
SELECT sum(1 + uint64 AS i) j from test_table where i > 0 having j > 0;
SELECT sum((1 AS m) + (uint64 AS n)) j from test_table where m > 0 and n > 0 having j > 0;
SELECT sum(((1 AS m) + (uint64 AS n)) AS i) j from test_table where m > 0 and n > 0 and i > 0 having j > 0;
EXPLAIN SYNTAX (SELECT sum(1 + uint64 AS i) from test_table where i > 0);
EXPLAIN SYNTAX (SELECT sum(1 + uint64) AS j from test_table having j > 0);
EXPLAIN SYNTAX (SELECT sum(1 + uint64 AS i) j from test_table where i > 0 having j > 0);
EXPLAIN SYNTAX (SELECT sum((1 AS m) + (uint64 AS n)) j from test_table where m > 0 and n > 0 having j > 0);
EXPLAIN SYNTAX (SELECT sum(((1 AS m) + (uint64 AS n)) AS i) j from test_table where m > 0 and n > 0 and i > 0 having j > 0);

SELECT sum(uint64 - 1 AS i) from test_table where i > 0;
SELECT sum(uint64 - 1) AS j from test_table having j > 0;
SELECT sum(uint64 - 1 AS i) j from test_table where i > 0 having j > 0;
SELECT sum((uint64 AS m) - (1 AS n)) j from test_table where m > 0 and n > 0 having j > 0;
SELECT sum(((uint64 AS m) - (1 AS n)) AS i) j from test_table where m > 0 and n > 0 and i > 0 having j > 0;
EXPLAIN SYNTAX (SELECT sum(uint64 - 1 AS i) from test_table where i > 0);
EXPLAIN SYNTAX (SELECT sum(uint64 - 1) AS j from test_table having j > 0);
EXPLAIN SYNTAX (SELECT sum(uint64 - 1 AS i) j from test_table where i > 0 having j > 0);
EXPLAIN SYNTAX (SELECT sum((uint64 AS m) - (1 AS n)) j from test_table where m > 0 and n > 0 having j > 0);
EXPLAIN SYNTAX (SELECT sum(((uint64 AS m) - (1 AS n)) AS i) j from test_table where m > 0 and n > 0 and i > 0 having j > 0);

SELECT sum(1 - uint64 AS i) from test_table;
SELECT sum(1 - uint64) AS j from test_table;
SELECT sum(1 - uint64 AS i) j from test_table;
SELECT sum((1 AS m) - (uint64 AS n)) j from test_table;
SELECT sum(((1 AS m) - (uint64 AS n)) AS i) j from test_table;
EXPLAIN SYNTAX (SELECT sum(1 - uint64 AS i) from test_table where i > 0);
EXPLAIN SYNTAX (SELECT sum(1 - uint64) AS j from test_table having j < 0);
EXPLAIN SYNTAX (SELECT sum(1 - uint64 AS i) j from test_table where i > 0 having j < 0);
EXPLAIN SYNTAX (SELECT sum((1 AS m) - (uint64 AS n)) j from test_table where m > 0 and n > 0 having j < 0);
EXPLAIN SYNTAX (SELECT sum(((1 AS m) - (uint64 AS n)) AS i) j from test_table where m > 0 and n > 0 and i < 0 having j < 0);

SELECT sum(uint64 + 2.11) From test_table;
SELECT sum(2.11 + uint64) From test_table;
SELECT sum(uint64 - 2.11) From test_table;
SELECT sum(2.11 - uint64) From test_table;
SELECT sum(uint64) + 2.11 * count(uint64) From test_table;
SELECT 2.11 * count(uint64) + sum(uint64) From test_table;
SELECT sum(uint64) - 2.11 * count(uint64) From test_table;
SELECT 2.11 * count(uint64) - sum(uint64) From test_table;
EXPLAIN SYNTAX (SELECT sum(uint64 + 2.11) From test_table);
EXPLAIN SYNTAX (SELECT sum(2.11 + uint64) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64 - 2.11) From test_table);
EXPLAIN SYNTAX (SELECT sum(2.11 - uint64) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64) + 2.11 * count(uint64) From test_table);
EXPLAIN SYNTAX (SELECT 2.11 * count(uint64) + sum(uint64) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64) - 2.11 * count(uint64) From test_table);
EXPLAIN SYNTAX (SELECT 2.11 * count(uint64) - sum(uint64) From test_table);

SELECT sum(uint64 + 2) From test_table;
SELECT sum(2 + uint64) From test_table;
SELECT sum(uint64 - 2) From test_table;
SELECT sum(2 - uint64) From test_table;
SELECT sum(uint64) + 2 * count(uint64) From test_table;
SELECT 2 * count(uint64) + sum(uint64) From test_table;
SELECT sum(uint64) - 2 * count(uint64) From test_table;
SELECT 2 * count(uint64) - sum(uint64) From test_table;
EXPLAIN SYNTAX (SELECT sum(uint64 + 2) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 + uint64) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64 - 2) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 - uint64) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64) + 2 * count(uint64) From test_table);
EXPLAIN SYNTAX (SELECT 2 * count(uint64) + sum(uint64) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64) - 2 * count(uint64) From test_table);
EXPLAIN SYNTAX (SELECT 2 * count(uint64) - sum(uint64) From test_table);

SELECT sum(float64 + 2) From test_table;
SELECT sum(2 + float64) From test_table;
SELECT sum(float64 - 2) From test_table;
SELECT sum(2 - float64) From test_table;
SELECT sum(float64) + 2 * count(float64) From test_table;
SELECT 2 * count(float64) + sum(float64) From test_table;
SELECT sum(float64) - 2 * count(float64) From test_table;
SELECT 2 * count(float64) - sum(float64) From test_table;
EXPLAIN SYNTAX (SELECT sum(float64 + 2) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 + float64) From test_table);
EXPLAIN SYNTAX (SELECT sum(float64 - 2) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 - float64) From test_table);
EXPLAIN SYNTAX (SELECT sum(float64) + 2 * count(float64) From test_table);
EXPLAIN SYNTAX (SELECT 2 * count(float64) + sum(float64) From test_table);
EXPLAIN SYNTAX (SELECT sum(float64) - 2 * count(float64) From test_table);
EXPLAIN SYNTAX (SELECT 2 * count(float64) - sum(float64) From test_table);

SELECT sum(decimal32 + 2) From test_table;
SELECT sum(2 + decimal32) From test_table;
SELECT sum(decimal32 - 2) From test_table;
SELECT sum(2 - decimal32) From test_table;
SELECT sum(decimal32) + 2 * count(decimal32) From test_table;
SELECT 2 * count(decimal32) + sum(decimal32) From test_table;
SELECT sum(decimal32) - 2 * count(decimal32) From test_table;
SELECT 2 * count(decimal32) - sum(decimal32) From test_table;
EXPLAIN SYNTAX (SELECT sum(decimal32 + 2) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 + decimal32) From test_table);
EXPLAIN SYNTAX (SELECT sum(decimal32 - 2) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 - decimal32) From test_table);
EXPLAIN SYNTAX (SELECT sum(decimal32) + 2 * count(decimal32) From test_table);
EXPLAIN SYNTAX (SELECT 2 * count(decimal32) + sum(decimal32) From test_table);
EXPLAIN SYNTAX (SELECT sum(decimal32) - 2 * count(decimal32) From test_table);
EXPLAIN SYNTAX (SELECT 2 * count(decimal32) - sum(decimal32) From test_table);

SELECT sum(uint64 + 2) + sum(uint64 + 3) From test_table;
SELECT sum(uint64 + 2) - sum(uint64 + 3) From test_table;
SELECT sum(uint64 - 2) + sum(uint64 - 3) From test_table;
SELECT sum(uint64 - 2) - sum(uint64 - 3) From test_table;
SELECT sum(2 - uint64) - sum(3 - uint64) From test_table;
SELECT (sum(uint64) + 2 * count(uint64)) + (sum(uint64) + 3 * count(uint64)) From test_table;
SELECT (sum(uint64) + 2 * count(uint64)) - (sum(uint64) + 3 * count(uint64)) From test_table;
SELECT (sum(uint64) - 2 * count(uint64)) + (sum(uint64) - 3 * count(uint64)) From test_table;
SELECT (sum(uint64) - 2 * count(uint64)) - (sum(uint64) - 3 * count(uint64)) From test_table;
SELECT (2 * count(uint64) - sum(uint64)) + (3 * count(uint64) - sum(uint64)) From test_table;
EXPLAIN SYNTAX (SELECT sum(uint64 + 2) + sum(uint64 + 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64 + 2) - sum(uint64 + 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64 - 2) + sum(uint64 - 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(uint64 - 2) - sum(uint64 - 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 - uint64) - sum(3 - uint64) From test_table);
EXPLAIN SYNTAX (SELECT (sum(uint64) + 2 * count(uint64)) + (sum(uint64) + 3 * count(uint64)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(uint64) + 2 * count(uint64)) - (sum(uint64) + 3 * count(uint64)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(uint64) - 2 * count(uint64)) + (sum(uint64) - 3 * count(uint64)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(uint64) - 2 * count(uint64)) - (sum(uint64) - 3 * count(uint64)) From test_table);
EXPLAIN SYNTAX (SELECT (2 * count(uint64) - sum(uint64)) + (3 * count(uint64) - sum(uint64)) From test_table);

SELECT sum(float64 + 2) + sum(float64 + 3) From test_table;
SELECT sum(float64 + 2) - sum(float64 + 3) From test_table;
SELECT sum(float64 - 2) + sum(float64 - 3) From test_table;
SELECT sum(float64 - 2) - sum(float64 - 3) From test_table;
SELECT sum(2 - float64) - sum(3 - float64) From test_table;
SELECT (sum(float64) + 2 * count(float64)) + (sum(float64) + 3 * count(float64)) From test_table;
SELECT (sum(float64) + 2 * count(float64)) - (sum(float64) + 3 * count(float64)) From test_table;
SELECT (sum(float64) - 2 * count(float64)) + (sum(float64) - 3 * count(float64)) From test_table;
SELECT (sum(float64) - 2 * count(float64)) - (sum(float64) - 3 * count(float64)) From test_table;
SELECT (2 * count(float64) - sum(float64)) + (3 * count(float64) - sum(float64)) From test_table;
EXPLAIN SYNTAX (SELECT sum(float64 + 2) + sum(float64 + 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(float64 + 2) - sum(float64 + 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(float64 - 2) + sum(float64 - 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(float64 - 2) - sum(float64 - 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 - float64) - sum(3 - float64) From test_table);
EXPLAIN SYNTAX (SELECT (sum(float64) + 2 * count(float64)) + (sum(float64) + 3 * count(float64)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(float64) + 2 * count(float64)) - (sum(float64) + 3 * count(float64)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(float64) - 2 * count(float64)) + (sum(float64) - 3 * count(float64)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(float64) - 2 * count(float64)) - (sum(float64) - 3 * count(float64)) From test_table);
EXPLAIN SYNTAX (SELECT (2 * count(float64) - sum(float64)) + (3 * count(float64) - sum(float64)) From test_table);

SELECT sum(decimal32 + 2) + sum(decimal32 + 3) From test_table;
SELECT sum(decimal32 + 2) - sum(decimal32 + 3) From test_table;
SELECT sum(decimal32 - 2) + sum(decimal32 - 3) From test_table;
SELECT sum(decimal32 - 2) - sum(decimal32 - 3) From test_table;
SELECT sum(2 - decimal32) - sum(3 - decimal32) From test_table;
SELECT (sum(decimal32) + 2 * count(decimal32)) + (sum(decimal32) + 3 * count(decimal32)) From test_table;
SELECT (sum(decimal32) + 2 * count(decimal32)) - (sum(decimal32) + 3 * count(decimal32)) From test_table;
SELECT (sum(decimal32) - 2 * count(decimal32)) + (sum(decimal32) - 3 * count(decimal32)) From test_table;
SELECT (sum(decimal32) - 2 * count(decimal32)) - (sum(decimal32) - 3 * count(decimal32)) From test_table;
SELECT (2 * count(decimal32) - sum(decimal32)) + (3 * count(decimal32) - sum(decimal32)) From test_table;
EXPLAIN SYNTAX (SELECT sum(decimal32 + 2) + sum(decimal32 + 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(decimal32 + 2) - sum(decimal32 + 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(decimal32 - 2) + sum(decimal32 - 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(decimal32 - 2) - sum(decimal32 - 3) From test_table);
EXPLAIN SYNTAX (SELECT sum(2 - decimal32) - sum(3 - decimal32) From test_table);
EXPLAIN SYNTAX (SELECT (sum(decimal32) + 2 * count(decimal32)) + (sum(decimal32) + 3 * count(decimal32)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(decimal32) + 2 * count(decimal32)) - (sum(decimal32) + 3 * count(decimal32)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(decimal32) - 2 * count(decimal32)) + (sum(decimal32) - 3 * count(decimal32)) From test_table);
EXPLAIN SYNTAX (SELECT (sum(decimal32) - 2 * count(decimal32)) - (sum(decimal32) - 3 * count(decimal32)) From test_table);
EXPLAIN SYNTAX (SELECT (2 * count(decimal32) - sum(decimal32)) + (3 * count(decimal32) - sum(decimal32)) From test_table);

-- https://github.com/ClickHouse/ClickHouse/issues/59414
SELECT sum(uint64 + 2) as j, j + 5 as t from test_table;
EXPLAIN SYNTAX SELECT sum(uint64 + 2) as j, j + 5 as t from test_table;
-- { echoOff }


DROP TABLE IF EXISTS test_table;
