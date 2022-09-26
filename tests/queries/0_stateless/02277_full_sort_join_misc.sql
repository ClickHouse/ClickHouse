SET join_algorithm = 'full_sorting_merge';

SELECT * FROM (SELECT 1 as key) AS t1 JOIN (SELECT 1 as key) t2 ON t1.key = t2.key;

SELECT * FROM (SELECT 1 as key) AS t1 JOIN (SELECT 1 as key) t2 USING key;

SELECT * FROM (SELECT 1 :: UInt32 as key) AS t1 FULL JOIN (SELECT 1 :: Nullable(UInt32) as key) t2 USING (key);

SELECT * FROM (SELECT 1 :: UInt32 as key) AS t1 FULL JOIN (SELECT NULL :: Nullable(UInt32) as key) t2 USING (key);

SELECT * FROM (SELECT 1 :: Int32 as key) AS t1 JOIN (SELECT 1 :: UInt32 as key) t2 ON t1.key = t2.key;

SELECT * FROM (SELECT -1 :: Nullable(Int32) as key) AS t1 FULL JOIN (SELECT 4294967295 :: UInt32 as key) t2 ON t1.key = t2.key;

SELECT * FROM (SELECT 'a' :: LowCardinality(String) AS key) AS t1 JOIN (SELECT 'a' :: String AS key) AS t2 ON t1.key = t2.key;

SELECT * FROM (SELECT 'a' :: LowCardinality(Nullable(String)) AS key) AS t1 JOIN (SELECT 'a' :: String AS key) AS t2 ON t1.key = t2.key;

SELECT * FROM (SELECT 'a' :: LowCardinality(Nullable(String)) AS key) AS t1 JOIN (SELECT 'a' :: Nullable(String) AS key) AS t2 ON t1.key = t2.key;

SELECT * FROM (SELECT 'a' :: LowCardinality(String) AS key) AS t1 JOIN (SELECT 'a' :: LowCardinality(String) AS key) AS t2 ON t1.key = t2.key;

SELECT 5 == count() FROM (SELECT number as a from numbers(5)) as t1 LEFT JOIN (SELECT number as b from numbers(5) WHERE number > 100) as t2 ON t1.a = t2.b;
SELECT 5 == count() FROM (SELECT number as a from numbers(5) WHERE number > 100) as t1 RIGHT JOIN (SELECT number as b from numbers(5)) as t2 ON t1.a = t2.b;
