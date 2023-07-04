SET join_algorithm = 'hash';

SELECT * FROM ( SELECT 2 AS x ) AS t1 RIGHT JOIN ( SELECT count('x'), count('y'), 2 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT 2 AS x ) as t1 RIGHT JOIN ( SELECT count('x'), count('y'), 0 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT 2 AS x ) as t1 RIGHT JOIN ( SELECT count('x') :: Nullable(Int32), count('y'), 0 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT 2 AS x ) as t1 RIGHT JOIN ( SELECT count('x') :: Nullable(Int32), count('y') :: Nullable(Int32), 0 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT count('a'), count('b'), count('c'), 2 AS x ) as t1 RIGHT JOIN ( SELECT  count('x'), count('y'), 0 AS x ) AS t2 ON t1.x = t2.x;

SELECT 'y', * FROM (SELECT count('y'), count('y'), 2 AS x) AS t1 RIGHT JOIN (SELECT count('x'), count('y'), 3 AS x) AS t2 ON t1.x = t2.x;
SELECT * FROM (SELECT arrayJoin([NULL]), 9223372036854775806, arrayJoin([NULL]), NULL AS x) AS t1 RIGHT JOIN (SELECT arrayJoin([arrayJoin([10000000000.])]), NULL AS x) AS t2 ON t1.x = t2.x;

SET join_algorithm = 'partial_merge';

SELECT * FROM ( SELECT 2 AS x ) AS t1 RIGHT JOIN ( SELECT count('x'), count('y'), 2 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT 2 AS x ) as t1 RIGHT JOIN ( SELECT count('x'), count('y'), 0 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT 2 AS x ) as t1 RIGHT JOIN ( SELECT count('x') :: Nullable(Int32), count('y'), 0 AS x ) AS t2 ON t1.x = t2.x;
SELECT * FROM ( SELECT 2 AS x ) as t1 RIGHT JOIN ( SELECT count('x') :: Nullable(Int32), count('y') :: Nullable(Int32), 0 AS x ) AS t2 ON t1.x = t2.x;

SELECT * FROM ( SELECT count('a'), count('b'), count('c'), 2 AS x ) as t1 RIGHT JOIN ( SELECT  count('x'), count('y'), 0 AS x ) AS t2 ON t1.x = t2.x;

SELECT 'y', * FROM (SELECT count('y'), count('y'), 2 AS x) AS t1 RIGHT JOIN (SELECT count('x'), count('y'), 3 AS x) AS t2 ON t1.x = t2.x;
SELECT * FROM (SELECT arrayJoin([NULL]), 9223372036854775806, arrayJoin([NULL]), NULL AS x) AS t1 RIGHT JOIN (SELECT arrayJoin([arrayJoin([10000000000.])]), NULL AS x) AS t2 ON t1.x = t2.x;
