DROP TABLE IF EXISTS test02315;

CREATE TABLE test02315(a UInt64, b UInt64) ENGINE=MergeTree() ORDER BY (a, b);

INSERT INTO test02315 SELECT number % 2 as a, number as b FROM numbers(10);

-- { echoOn }
SELECT count() AS amount, a, b, GROUPING(a, b) FROM test02315 GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY (amount, a, b);

SELECT count() AS amount, a, b, GROUPING(a, b) FROM test02315 GROUP BY ROLLUP(a, b) ORDER BY (amount, a, b);

-- { echoOff }
DROP TABLE test02315;
