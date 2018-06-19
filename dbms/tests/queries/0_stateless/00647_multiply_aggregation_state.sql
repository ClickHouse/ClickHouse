SELECT countMerge(x) AS y FROM ( SELECT countState() * 2 AS x FROM ( SELECT 1 ));
SELECT countMerge(x) AS y FROM ( SELECT countState() * 0 AS x FROM ( SELECT 1 UNION ALL SELECT 2));
SELECT sumMerge(y) AS z FROM ( SELECT sumState(x) * 11 AS y FROM ( SELECT 1 AS x UNION ALL SELECT 2 AS x));
SELECT countMerge(x) AS y FROM ( SELECT 2 * countState() AS x FROM ( SELECT 1 ));
SELECT countMerge(x) AS y FROM ( SELECT 0 * countState() AS x FROM ( SELECT 1 UNION ALL SELECT 2));
SELECT sumMerge(y) AS z FROM ( SELECT 3 * sumState(x) * 2 AS y FROM ( SELECT 1 AS x UNION ALL SELECT 2 AS x));
