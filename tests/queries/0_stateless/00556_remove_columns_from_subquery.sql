SELECT a FROM (SELECT 1 AS a, 2 AS b);
SELECT a FROM (SELECT 1 AS a, arrayJoin([2, 3]) AS b);
SELECT a FROM (SELECT 1 AS a, arrayJoin([2, 3]), arrayJoin([2, 3]));
SELECT a FROM (SELECT 1 AS a, arrayJoin([2, 3]), arrayJoin([4, 5]));
SELECT a, b FROM (SELECT a, * FROM (SELECT 1 AS a, 2 AS b, 3 AS c));
SELECT a, b FROM (SELECT a, *, arrayJoin(c) FROM (SELECT 1 AS a, 2 AS b, [3, 4] AS c));
