SELECT has([(a, b), (c, d)], (a, b)) FROM (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (c, d)) FROM (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT 1 AS a, 2 AS b, 2 AS c, 2 AS d);

SELECT has([(a, b), (c, d)], (a, b)) FROM (SELECT number + 1 AS a, number + 2 AS b, number + 3 AS c, number + 4 AS d FROM system.numbers LIMIT 2);
SELECT has([(a, b), (c, d)], (c, d)) FROM (SELECT number + 1 AS a, number + 2 AS b, number + 3 AS c, number + 4 AS d FROM system.numbers LIMIT 2);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT number + 1 AS a, number + 2 AS b, number + 3 AS c, number + 4 AS d FROM system.numbers LIMIT 2);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT number + 1 AS a, number + 2 AS b, number + 2 AS c, number + 2 AS d FROM system.numbers LIMIT 2);

SELECT has([(a, b), (c, d)], (a, b)) FROM (SELECT materialize(1) AS a, 2 AS b, 3 AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (c, d)) FROM (SELECT materialize(1) AS a, 2 AS b, 3 AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT materialize(1) AS a, 2 AS b, 3 AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT materialize(1) AS a, 2 AS b, 2 AS c, 2 AS d);

SELECT has([(a, b), (c, d)], (a, b)) FROM (SELECT materialize(1) AS a, 2 AS b, materialize(3) AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (c, d)) FROM (SELECT materialize(1) AS a, 2 AS b, materialize(3) AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT materialize(1) AS a, 2 AS b, materialize(3) AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT materialize(1) AS a, 2 AS b, materialize(2) AS c, 2 AS d);

SELECT has([(a, b), (c, d)], (a, b)) FROM (SELECT materialize(1) AS a, materialize(2) AS b, materialize(3) AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (c, d)) FROM (SELECT materialize(1) AS a, materialize(2) AS b, materialize(3) AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT materialize(1) AS a, materialize(2) AS b, materialize(3) AS c, 4 AS d);
SELECT has([(a, b), (c, d)], (b, c)) FROM (SELECT materialize(1) AS a, materialize(2) AS b, materialize(2) AS c, 2 AS d);
