-- Regression test: CROSS JOIN combined with INNER JOIN USING caused
-- a logical error in CollectSourceColumnsVisitor because CROSS_JOIN
-- was not handled as a valid column source node type.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=1aefdf9c553447757c0daa4a6d48fa875173b7ee&name_0=MasterCI&name_1=BuzzHouse%20%28amd_ubsan%29

SELECT c0
FROM (SELECT 1 AS c0) AS t0
CROSS JOIN (SELECT 2 AS c1) AS t1
INNER JOIN (SELECT 1 AS c0) AS t2 USING (c0);

SELECT *
FROM (SELECT 1 AS c0, 10 AS c1) AS t0
CROSS JOIN (SELECT 2 AS c2) AS t1
INNER JOIN (SELECT 1 AS c0) AS t2 USING (c0);

SELECT c0
FROM (SELECT 1 AS c0) AS t0
CROSS JOIN (SELECT 2 AS c1) AS t1
CROSS JOIN (SELECT 3 AS c2) AS t2
INNER JOIN (SELECT 1 AS c0) AS t3 USING (c0);
