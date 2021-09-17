SET max_block_size = 6, join_algorithm = 'partial_merge';

SELECT count() == 4  FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2]) AS s) AS js2 USING (s);
SELECT count() == 5  FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2]) AS s) AS js2 USING (s);
SELECT count() == 6  FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2]) AS s) AS js2 USING (s);
SELECT count() == 7  FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2]) AS s) AS js2 USING (s);
SELECT count() == 8  FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3]) AS s) AS js2 USING (s);
SELECT count() == 9  FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 10 FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 11 FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 12 FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 13 FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 14 FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 15 FROM (SELECT 1 AS s) AS js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3]) AS s) AS js2 USING (s);

SELECT count() == 8  FROM (SELECT 1 AS s) AS js1 FULL JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2]) AS s) AS js2 USING (s);
SELECT count() == 9  FROM (SELECT 1 AS s) AS js1 FULL JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3]) AS s) AS js2 USING (s);
SELECT count() == 10 FROM (SELECT 1 AS s) AS js1 FULL JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3]) AS s) AS js2 USING (s);
SELECT count() == 11 FROM (SELECT 1 AS s) AS js1 FULL JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3]) AS s) AS js2 USING (s);
