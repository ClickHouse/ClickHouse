
SET query_cache_tag = '02494_query_cache_totals_extremes';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_totals_extremes';
DROP TABLE IF EXISTS tbl;

CREATE TABLE tbl (key UInt64, agg UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO tbl VALUES (1, 3), (2, 2), (1, 4), (1, 1);

-- A query with totals calculation. The result should be written into / read from the query cache.
-- Check that both queries produce the same result and that a query cache entry exists.
SELECT '1st run:';
SELECT key, sum(agg) FROM tbl GROUP BY key WITH totals ORDER BY key SETTINGS use_query_cache = 1;
SELECT '2nd run:';
SELECT key, sum(agg) FROM tbl GROUP BY key WITH totals ORDER BY key SETTINGS use_query_cache = 1;

SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_totals_extremes') AS test_query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_totals_extremes';

-- A query with extremes calculation. The result should be written into / read from the query cache.
-- Check that both queries produce the same result.
SELECT '1st run:';
SELECT key, sum(agg) FROM tbl GROUP BY key ORDER BY key SETTINGS use_query_cache = 1, extremes = 1;
SELECT '2nd run:';
SELECT key, sum(agg) FROM tbl GROUP BY key ORDER BY key SETTINGS use_query_cache = 1, extremes = 1;

SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_totals_extremes') AS test_query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_totals_extremes';

-- A query with totals and extremes calculation. The result should be written into / read from the query cache.
-- Check that both queries produce the same result.
SELECT '1st run:';
SELECT key, sum(agg) FROM tbl GROUP BY key WITH totals ORDER BY key SETTINGS use_query_cache = 1, extremes = 1;
SELECT '2nd run:';
SELECT key, sum(agg) FROM tbl GROUP BY key WITH totals ORDER BY key SETTINGS use_query_cache = 1, extremes = 1;

SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_totals_extremes') AS test_query_cache;
DROP TABLE IF EXISTS tbl;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_totals_extremes';
