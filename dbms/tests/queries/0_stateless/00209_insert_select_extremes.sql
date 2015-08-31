DROP TABLE IF EXISTS test.test;
CREATE TABLE test.test (x UInt8) ENGINE = Log;

INSERT INTO test.test SELECT 1 AS x;
INSERT INTO test.test SELECT 1 AS x SETTINGS extremes = 1;
INSERT INTO test.test SELECT 1 AS x GROUP BY 1 WITH TOTALS;
INSERT INTO test.test SELECT 1 AS x GROUP BY 1 WITH TOTALS SETTINGS extremes = 1;

SELECT count(), min(x), max(x) FROM test.test;

DROP TABLE test.test;
