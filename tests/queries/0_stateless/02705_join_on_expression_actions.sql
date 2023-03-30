
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

DROP TABLE IF EXISTS numbers_l;
DROP TABLE IF EXISTS numbers_r;

CREATE TABLE numbers_l (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO numbers_l SELECT sipHash64(number, 11) % 32 as a, sipHash64(number, 12) % 128 as b, sipHash64(number, 13) % 128 as c, sipHash64(number, 14) % 128 as d FROM numbers(10_000);

CREATE TABLE numbers_r (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO numbers_r SELECT sipHash64(number, 21) % 32 as a, sipHash64(number, 22) % 128 as b, sipHash64(number, 23) % 128 as c, sipHash64(number, 24) % 128 as d FROM numbers(10_000);

-- refernce taken from the result of CROSS JOIN ... WHERE

SELECT '---';

SELECT count() == 1532734 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
;

SELECT sum(t1.d) == 96884463, sum(t2.d) == 96771783 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
;

SELECT count() == 761141 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
;

SELECT sum(t1.d) == 64749616, sum(t2.d) == 48112729 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
;

SELECT count() == 268812 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
;

SELECT sum(t1.d) == 22888318, sum(t2.d) == 17101297 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
;

SELECT count() == 45 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
;

SELECT sum(t1.d) == 3943, sum(t2.d) == 2812 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
;

SELECT count() == 25 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
    AND t2.d % 2 == 0
;

SELECT sum(t1.d) == 2192, sum(t2.d) == 1620 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
    AND t2.d % 2 == 0
;

SELECT count() == 23 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
    AND t2.d % 2 == 0
    AND (t2.b % 2 == 0 OR t1.b % 2 == 0)
;

SELECT sum(t1.d) == 2036, sum(t2.d) == 1508 FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
    AND t2.d % 2 == 0
    AND (t2.b % 2 == 0 OR t1.b % 2 == 0)
;

SELECT * FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
    AND t2.d % 2 == 0
    AND (t2.b % 2 == 0 OR t1.b % 2 == 0)
    AND t2.d < t1.d
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
;
SELECT '---';

SELECT * FROM numbers_l AS t1 JOIN numbers_r as t2
ON
    t1.b < t2.b
    AND t1.a = t2.a
    AND (t2.c < t1.d OR t1.d >= t2.c * 2)
    AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0)
    AND t1.a * t2.a == t1.b * t2.b
    AND t2.d % 2 == 0
    AND (t2.b % 2 == 0 OR t1.b % 2 == 0)
    AND t1.b + 30 = t2.b
;

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
DROP TABLE IF EXISTS numbers_r;
DROP TABLE IF EXISTS numbers_l;
