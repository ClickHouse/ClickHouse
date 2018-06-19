SELECT countMerge(x) AS y FROM ( SELECT countState() * 2 AS x FROM ( SELECT 1 ));
SELECT countMerge(x) AS y FROM ( SELECT countState() * 0 AS x FROM ( SELECT 1 UNION ALL SELECT 2));
SELECT sumMerge(y) AS z FROM ( SELECT sumState(x) * 11 AS y FROM ( SELECT 1 AS x UNION ALL SELECT 2 AS x));
