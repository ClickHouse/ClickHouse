CREATE TABLE IF NOT EXISTS data (sketch Array(Int8)) ENGINE=Memory;

INSERT INTO data VALUES ([-1,-1,-1]), ([4,-1,2]), ([0,25,-1]), ([-1,-1,7]), ([-1,-1,-1]);

SELECT max(sketch) FROM data;

SELECT maxArray(sketch) FROM data;

SELECT maxForEach(sketch) FROM data;

DROP TABLE data;


SELECT k, sumForEach(arr) FROM (SELECT number % 3 AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, sumForEach(arr) FROM (SELECT intDiv(number, 3) AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, groupArrayForEach(arr) FROM (SELECT number % 3 AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, groupArrayForEach(arr) FROM (SELECT intDiv(number, 3) AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, groupArrayForEach(arr) FROM (SELECT number % 3 AS k, arrayMap(x -> toString(x), range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, groupArrayForEach(arr) FROM (SELECT intDiv(number, 3) AS k, arrayMap(x -> toString(x), range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, groupArrayForEach(arr), quantilesExactForEach(0.5, 0.9)(arr) FROM (SELECT intDiv(number, 3) AS k, arrayMap(x -> number + x, range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT uniqForEach(x) FROM (SELECT emptyArrayUInt8() AS x UNION ALL SELECT [1, 2, 3] UNION ALL SELECT emptyArrayUInt8() UNION ALL SELECT [2, 2]);
