SET allow_experimental_analyzer = 1;

SELECT 1 + arrayJoin(a) AS m FROM (SELECT [1, 2, 3] AS a) GROUP BY m;

SELECT 1 + arrayJoin(a) AS m FROM (SELECT [1, 2, 3] AS a) GROUP BY 1 + arrayJoin(a);
