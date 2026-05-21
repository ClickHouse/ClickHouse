-- { echoOn }

SET enable_analyzer = 1;

SELECT 1 IN (tuple(tuple(1, 2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT number FROM numbers(10) WHERE number % 2 IN (number % 3, number % 5) ORDER BY number;
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number;
SELECT number FROM numbers(3) WHERE number IN (number + 1) ORDER BY number;
SELECT number, number % 3 IN (number % 2, 1), number % 3 NOT IN (number % 2, 1) FROM numbers(6) ORDER BY number;
SELECT toFloat64(-0.0) IN (toFloat64(0.0)), toFloat64(-0.0) NOT IN (toFloat64(0.0)), toFloat64(-0.0) IN (toFloat64(number * 0)), toFloat64(-0.0) NOT IN (toFloat64(number * 0)), toFloat64(-0.0) IN [toFloat64(number * 0)], toFloat64(-0.0) NOT IN [toFloat64(number * 0)] FROM numbers(1);
SELECT number, number % 3 IN arrayMap(x -> x + number % 2, [0, 1]), number % 3 NOT IN arrayMap(x -> x + number % 2, [0, 1]) FROM numbers(6) ORDER BY number;
SELECT number, number % 3 GLOBAL IN (number % 2, 1), number % 3 GLOBAL NOT IN (number % 2, 1) FROM numbers(6) ORDER BY number;
SELECT number, (number % 2, number % 3) IN ((number % 3, number % 2), (1, 1)), (number % 2, number % 3) NOT IN ((number % 3, number % 2), (1, 1)) FROM numbers(6) ORDER BY number;
SELECT number, (number, number + 1) IN [tuple(number, number + 1)], (number, number + 2) IN [tuple(number, number + 1)] FROM numbers(3) ORDER BY number;
SELECT number, if(number >= 0, tuple(number, number + 1), tuple(0, 0)) AS t, number IN (t), (number + 1) IN (t), (number + 2) IN (t), (number, number + 1) IN (t) FROM numbers(3) ORDER BY number;
SELECT number, tuple(number, number + 1) AS t, number IN (t), (number + 1) IN (t), (number + 2) IN (t), (number, number + 1) IN (t) FROM numbers(3) ORDER BY number;
SELECT number, (number, number + 1) IN ((number, number + 1)), (number, number + 2) IN ((number, number + 1)) FROM numbers(3) ORDER BY number;

SELECT x IN (y, 1), x NOT IN (y, 1), x IN [y, 1], x NOT IN [y, 1]
FROM (SELECT materialize(NULL) AS x, materialize(2) AS y);

SELECT x IN (y, 1), x NOT IN (y, 1), x IN [y, 1], x NOT IN [y, 1]
FROM (SELECT materialize(NULL) AS x, materialize(NULL) AS y)
SETTINGS transform_null_in = 1;

SET enable_analyzer = 0;

SELECT number FROM numbers(10) WHERE number % 2 IN (number % 3, number % 5) ORDER BY number;
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number;
SELECT number FROM numbers(3) WHERE number IN (number + 1) ORDER BY number;
SELECT number, number % 3 IN (number % 2, 1), number % 3 NOT IN (number % 2, 1) FROM numbers(6) ORDER BY number;
SELECT toFloat64(-0.0) IN (toFloat64(0.0)), toFloat64(-0.0) NOT IN (toFloat64(0.0)), toFloat64(-0.0) IN (toFloat64(number * 0)), toFloat64(-0.0) NOT IN (toFloat64(number * 0)), toFloat64(-0.0) IN [toFloat64(number * 0)], toFloat64(-0.0) NOT IN [toFloat64(number * 0)] FROM numbers(1);
SELECT number, number % 3 IN arrayMap(x -> x + number % 2, [0, 1]), number % 3 NOT IN arrayMap(x -> x + number % 2, [0, 1]) FROM numbers(6) ORDER BY number;
SELECT number, number % 3 GLOBAL IN (number % 2, 1), number % 3 GLOBAL NOT IN (number % 2, 1) FROM numbers(6) ORDER BY number;
SELECT number, (number % 2, number % 3) IN ((number % 3, number % 2), (1, 1)), (number % 2, number % 3) NOT IN ((number % 3, number % 2), (1, 1)) FROM numbers(6) ORDER BY number;
SELECT number, (number, number + 1) IN [tuple(number, number + 1)], (number, number + 2) IN [tuple(number, number + 1)] FROM numbers(3) ORDER BY number;
SELECT number, if(number >= 0, tuple(number, number + 1), tuple(0, 0)) AS t, number IN (t), (number + 1) IN (t), (number + 2) IN (t), (number, number + 1) IN (t) FROM numbers(3) ORDER BY number;
SELECT number, tuple(number, number + 1) AS t, number IN (t), (number + 1) IN (t), (number + 2) IN (t), (number, number + 1) IN (t) FROM numbers(3) ORDER BY number;
SELECT number, (number, number + 1) IN ((number, number + 1)), (number, number + 2) IN ((number, number + 1)) FROM numbers(3) ORDER BY number;

SELECT x IN (y, 1), x NOT IN (y, 1), x IN [y, 1], x NOT IN [y, 1]
FROM (SELECT materialize(NULL) AS x, materialize(2) AS y);

SELECT x IN (y, 1), x NOT IN (y, 1), x IN [y, 1], x NOT IN [y, 1]
FROM (SELECT materialize(NULL) AS x, materialize(NULL) AS y)
SETTINGS transform_null_in = 1;
