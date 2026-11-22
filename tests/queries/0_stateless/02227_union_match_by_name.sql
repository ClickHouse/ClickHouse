SET enable_analyzer = 1;

-- { echoOn }

EXPLAIN header = 1, optimize = 0 SELECT avgWeighted(x, y) FROM (SELECT NULL, 255 AS x, 1 AS y UNION ALL SELECT y, NULL AS x, 1 AS y);
SELECT avgWeighted(x, y) FROM (SELECT NULL, 255 AS x, 1 AS y UNION ALL SELECT y, NULL AS x, 1 AS y);

-- { echoOff }
