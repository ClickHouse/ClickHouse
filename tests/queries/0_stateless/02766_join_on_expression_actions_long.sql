-- Tags: long

DROP TABLE IF EXISTS numbers_l;
DROP TABLE IF EXISTS numbers_r;

CREATE TABLE numbers_l (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO numbers_l SELECT sipHash64(number, 11) % 32 as a, sipHash64(number, 12) % 128 as b, sipHash64(number, 13) % 128 as c, sipHash64(number, 14) % 128 as d FROM numbers(10_000);

CREATE TABLE numbers_r (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO numbers_r SELECT sipHash64(number, 21) % 32 as a, sipHash64(number, 22) % 128 as b, sipHash64(number, 23) % 128 as c, sipHash64(number, 24) % 128 as d FROM numbers(10_000);

SET allow_experimental_analyzer = 1;

-- refernce taken from the result of CROSS JOIN ... WHERE

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

DROP TABLE IF EXISTS numbers_r;
DROP TABLE IF EXISTS numbers_l;
