SELECT (values['a']) AS x FROM (SELECT map('a', 42) AS values);
SELECT (values['a'] - 1) AS x FROM (SELECT map('a', 42) AS values);
SELECT (VALUES['a']) AS x FROM (SELECT map('a', 43) AS VALUES);

SELECT * FROM (VALUES (1, 'a'), (2, 'b')) ORDER BY c1;
