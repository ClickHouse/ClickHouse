-- A lambda in the SELECT list of a query with LIMIT AFTER or LIMIT UNTIL, read locally and from remote shards.

-- The boundary expression repeats a lambda of the SELECT list.
SELECT number, arraySum(x -> x, range(number)) FROM numbers(6) LIMIT AFTER arraySum(x -> x, range(number + 1)) > 3;
SELECT number, arraySum(x -> x, range(number)) FROM numbers(6) LIMIT UNTIL arraySum(x -> x, range(number + 1)) > 3;
SELECT number, arraySum(x -> x, range(number)) FROM numbers(6) LIMIT AFTER arraySum(x -> x, range(number + 1)) > 1 UNTIL arraySum(x -> x, range(number + 2)) > 10;

-- The boundary expression does not use the lambda.
SELECT number, arraySum(x -> x, range(number)) FROM numbers(6) LIMIT AFTER number > 2;

-- Not a reproduction, this shape works without the fix: an IN subquery in both the SELECT list and the boundary.
SELECT number, number IN (SELECT 3) FROM numbers(6) LIMIT AFTER (number + 1) IN (SELECT 3);

-- Remote shards. The read really goes through a remote source.
SELECT max(explain LIKE '%Remote%') FROM (EXPLAIN PIPELINE graph = 1 SELECT arrayExists(x -> (x IN (SELECT 2)), [2]) FROM remote('127.0.0.{2,3}', system.one) LIMIT UNTIL dummy = 1);
SELECT arrayExists(x -> (x IN (SELECT 2)), [2]) FROM remote('127.0.0.{2,3}', system.one) LIMIT UNTIL dummy = 1;
SELECT length(arrayMap(x -> x + rand(x), [dummy])) FROM remote('127.0.0.{2,3}', system.one) LIMIT AFTER dummy = 0;
