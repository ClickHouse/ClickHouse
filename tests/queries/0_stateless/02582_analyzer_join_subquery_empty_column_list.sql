SET enable_analyzer = 1;
-- { echoOn }

SELECT a FROM ( select 1 AS a ) AS t1, ( select 2 AS b, 3 AS c) AS t2;
SELECT a FROM ( select 1 AS a UNION ALL select 1 as a ) AS t1, ( select 2 AS b, 3 AS c) AS t2;
SELECT a FROM ( select 1 AS a ) AS t1, ( select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c) AS t2;
SELECT a FROM ( select 1 AS a UNION ALL select 1 as a ) AS t1, ( select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c) AS t2;
SELECT a FROM ( select * from ( select 1 AS a UNION ALL select 1 as a) ) AS t1, ( select * from (  select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c )) AS t2;
SELECT b FROM ( select 1 AS a UNION ALL select 1 as a ) AS t1, ( select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c) AS t2;
SELECT c FROM ( select 1 AS a UNION ALL select 1 as a ) AS t1, ( select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c) AS t2;
SELECT 42 FROM ( select 1 AS a UNION ALL select 1 as a ) AS t1, ( select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c) AS t2;
SELECT count() FROM ( select 1 AS a UNION ALL select 1 as a ) AS t1, ( select 2 AS b, 3 AS c UNION ALL select 2 as b, 3 as c) AS t2;
