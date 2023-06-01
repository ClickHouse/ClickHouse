
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key String, attr String, a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1 VALUES ('key1', 'a', 1, 1, 2), ('key1', 'b', 2, 3, 2), ('key1', 'c', 3, 2, 1), ('key1', 'd', 4, 7, 2), ('key1', 'e', 5, 5, 5), ('key2', 'a2', 1, 1, 1);

CREATE TABLE t11 (key String, attr String, a UInt64, b UInt64, c UInt64) ENGINE = TinyLog;
INSERT INTO t11 VALUES ('key1', 'a', 1, 1, 2), ('key1', 'b', 2, 3, 2), ('key1', 'c', 3, 2, 1), ('key1', 'd', 4, 7, 2), ('key1', 'e', 5, 5, 5), ('key2', 'a2', 1, 1, 1);
INSERT INTO t11 SELECT 'non_matched_key' || toString(number + 2048) , 'attr' || toString(number) , number , number , number FROM numbers(1024);

CREATE TABLE t2 (key String, attr String, a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t2 VALUES ('key1', 'A', 1, 2, 1), ('key1', 'B', 2, 1, 2), ('key1', 'C', 3, 4, 5), ('key1', 'D', 4, 1, 6), ('key3', 'a3', 1, 1, 1);
INSERT INTO t2 SELECT 'non_matched_key' || toString(number) , 'attr' || toString(number) , number , number , number FROM numbers(1024);

-- { echoOn }

SELECT t1.attr, t2.attr FROM t1 LEFT  JOIN t2 ON (t1.a < t2.a OR lower(t1.attr) == lower(t2.attr)) AND t1.key = t2.key ORDER BY (t1.attr, t2.attr);
SELECT t1.attr, t2.attr FROM t1 INNER JOIN t2 ON (t1.a < t2.a OR lower(t1.attr) == lower(t2.attr)) AND t1.key = t2.key ORDER BY (t1.attr, t2.attr);
SELECT t1.attr, t2.attr FROM t11 as t1 INNER JOIN t2 ON (t1.a < t2.a OR lower(t1.attr) == lower(t2.attr)) AND t1.key = t2.key ORDER BY (t1.attr, t2.attr);

SELECT t1.attr, t2.attr FROM t1 LEFT  JOIN t2 ON t1.key = t2.key AND t1.b + t2.b == t1.c + t2.c ORDER BY t1.attr, t2.attr;
SELECT t1.attr, t2.attr FROM t1 INNER JOIN t2 ON t1.key = t2.key AND t1.b + t2.b == t1.c + t2.c ORDER BY t1.attr, t2.attr;
SELECT t1.attr, t2.attr FROM t11 as t1 INNER JOIN t2 ON t1.key = t2.key AND t1.b + t2.b == t1.c + t2.c ORDER BY t1.attr, t2.attr;

SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a;
SELECT count() FROM t1 JOIN t2 ON 1 = 0 AND t1.a < t2.a;

-- { echoOff }

-- test error messages

SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a OR t1.a = t2.a; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 RIGHT JOIN t2 ON t1.key = t2.key AND t1.a < t2.a; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 FULL JOIN t2 ON t1.key = t2.key AND t1.a < t2.a; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 ANY JOIN t2 ON t1.key = t2.key AND t1.a < t2.a; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON 1 = 1 AND t1.a < t2.a; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM t1 ASOF JOIN t2 ON t1.key = t2.key AND if(ignore(t1.a), 1, t1.a < t2.a); -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM t1 ASOF JOIN t2 ON t1.key = t2.key AND t1.a < t2.a AND if(ignore(t1.a), 1, t1.a < t2.a); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS join_algorithm = 'partial_merge'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS join_algorithm = 'grace_hash'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS join_algorithm = 'parallel_hash'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS join_algorithm = 'auto'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS join_algorithm = 'direct'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t1 JOIN t2 ON t1.key = t2.key AND t1.a < t2.a SETTINGS allow_experimental_analyzer = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
