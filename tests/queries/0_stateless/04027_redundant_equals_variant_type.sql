-- Regression test: tryOptimizeOutRedundantEquals should not crash
-- when boolean function returns Variant type instead of UInt8.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=98660&sha=60f5f5859ae1f98998215ff346a914aaee674ff7&name_0=PR&name_1=BuzzHouse%20%28amd_debug%29
SET enable_analyzer = 1;

SELECT count() FROM (SELECT c0 FROM ((SELECT 'a') EXCEPT ALL SELECT (1, 2))(c0)) AS t0 WHERE t0.c0 ILIKE t0.c0 = true;

SELECT count() FROM (SELECT CAST('a', 'Variant(String, UInt8)') AS x) WHERE (x ILIKE x) = true;
SELECT count() FROM (SELECT CAST('a', 'Variant(String, UInt8)') AS x) WHERE (x ILIKE x) = false;
