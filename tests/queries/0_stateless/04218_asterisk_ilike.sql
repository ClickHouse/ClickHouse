SELECT * ILIKE 'foo%' FROM (SELECT 1 AS foo, 2 AS bar, 3 AS FooBar);
SELECT t.* ILIKE '%id' FROM (SELECT 1 AS UserID, 2 AS name, 3 AS session_id) AS t;
SELECT * ILIKE 'FOO_' FROM (SELECT 1 AS foo1, 2 AS foo2, 3 AS foo22, 4 AS bar);
SELECT * ILIKE '%' FROM (SELECT 1 AS a, 2 AS b, 3 AS c);
SELECT * ILIKE 'a*c' FROM (SELECT 1 AS `a*c`, 2 AS abc, 3 AS `a.c`);
SELECT * ILIKE 'данные%' FROM (SELECT 1 AS `данные`, 2 AS `данные_2`, 3 AS data);
SELECT * ILIKE 'user %' FROM (SELECT 1 AS `user id`, 2 AS `user name`, 3 AS userid);
SELECT * ILIKE 'metric_%' FROM (SELECT 1 AS metric_value, 2 AS `metric%value`, 3 AS metricXvalue, 4 AS other);
SELECT * ILIKE 'metric\\_%' FROM (SELECT 1 AS metric_value, 2 AS `metric%value`, 3 AS metricXvalue);
SELECT * ILIKE 'metric\\%%' FROM (SELECT 1 AS `metric%value`, 2 AS metric_value, 3 AS `METRIC%`);
SELECT * ILIKE 'foo%' EXCEPT (foo_extra) FROM (SELECT 1 AS foo, 2 AS foo_extra, 3 AS bar);
SELECT * ILIKE 'missing%' FROM (SELECT 1 AS foo); -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }
