-- Tags: long

DROP TABLE IF EXISTS numbers_l;
DROP TABLE IF EXISTS numbers_r;

CREATE TABLE numbers_l (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO numbers_l SELECT sipHash64(number, 11) % 32 as a, sipHash64(number, 12) % 128 as b, sipHash64(number, 13) % 128 as c, sipHash64(number, 14) % 128 as d FROM numbers(10_000);

CREATE TABLE numbers_r (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO numbers_r SELECT sipHash64(number, 21) % 32 as a, sipHash64(number, 22) % 128 as b, sipHash64(number, 23) % 128 as c, sipHash64(number, 24) % 128 as d FROM numbers(10_000);

SET allow_experimental_analyzer = 1;
SET join_algorithm = 'hash';

-- { echoOn }
SELECT count(1) == 1542059 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a;

SELECT sum(t1.d) == 98610126, sum(t2.d) == 97232272 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a;

SELECT count(1) == 775871 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2);

SELECT sum(t1.d) == 66052398, sum(t2.d) == 48936042 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2);

SELECT count(1) == 260711 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0);

SELECT sum(t1.d) == 22054037, sum(t2.d) == 16329655 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0);

SELECT count(1) == 280 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b;

SELECT sum(t1.d) == 30451, sum(t2.d) == 17512 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b;

SELECT count(1) == 120 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b AND t2.d % 2 == 0;

SELECT sum(t1.d) == 13614, sum(t2.d) == 8328 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b AND t2.d % 2 == 0;

SELECT count(1) == 118 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b AND t2.d % 2 == 0 AND (t2.b % 2 == 0 OR t1.b % 2 == 0);

SELECT sum(t1.d) == 13394, sum(t2.d) == 8132 FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b AND t2.d % 2 == 0 AND (t2.b % 2 == 0 OR t1.b % 2 == 0);

SELECT * FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b AND t2.d % 2 == 0 AND (t2.b % 2 == 0 OR t1.b % 2 == 0) AND t2.d < t1.d ORDER BY 1, 2, 3, 4, 5, 6, 7, 8;

SELECT * FROM numbers_l AS t1 JOIN numbers_r as t2 ON t1.b < t2.b AND t1.a = t2.a AND (t2.c < t1.d OR t1.d >= t2.c * 2) AND if(t1.c % 2 = 0, t1.c % 3 == 0, t2.c % 3 == 0) AND t1.a * t2.a == t1.b * t2.b AND t2.d % 2 == 0 AND (t2.b % 2 == 0 OR t1.b % 2 == 0) AND t1.b + 30 = t2.b;
-- { echoOff }



DROP TABLE IF EXISTS numbers_r;
DROP TABLE IF EXISTS numbers_l;
