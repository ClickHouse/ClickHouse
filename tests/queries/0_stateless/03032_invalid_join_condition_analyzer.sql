DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree ORDER BY key;
insert into t1 Select number, number from numbers(10);
insert into t2 Select number, number from numbers(10);

SET allow_experimental_analyzer = 1;

SELECT * FROM t1 WHERE 0 OR inf; -- { serverError CANNOT_CONVERT_TYPE }
SELECT * FROM t1 WHERE a OR inf; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT * FROM t1 WHERE 0 OR nan; -- { serverError CANNOT_CONVERT_TYPE }
SELECT * FROM t1 WHERE a OR nan; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT * FROM t1 WHERE a OR 1 OR inf ORDER BY a;
SELECT * FROM t1 WHERE a OR 1 OR nan ORDER BY a;

SELECT *
FROM t1
FULL OUTER JOIN t2 ON (t1.a = t2.key) OR inf; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT *
FROM t1
FULL OUTER JOIN t2 ON (t1.a = t2.key) OR nan; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- query rewritten to CROSS join since join condition is always true
select trimLeft(explain) from ( explain plan actions=1 SELECT *
FROM t1
FULL OUTER JOIN t2 ON (t1.a = t2.key) OR 1 OR inf)
where explain ilike '%Type: CROSS%';

-- DROP TABLE t2;
-- DROP TABLE t1;
