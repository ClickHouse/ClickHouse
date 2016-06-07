SELECT k, x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, x FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;

SELECT k, x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, x FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
