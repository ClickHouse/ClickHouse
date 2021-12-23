-- exponentialMovingAverage
SELECT number = 0 AS value, number AS time, exponentialMovingAverage(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);
SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialMovingAverage(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));

SELECT number AS value, number AS time, exponentialMovingAverage(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 1, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialMovingAverage(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 1, 50) AS bar
FROM
(
    SELECT
        (number % 5) = 0 AS value,
        number AS time,
        exponentialMovingAverage(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

-- exponentialTimeDecayedSum
SELECT number = 0 AS value, number AS time, exponentialTimeDecayedSum(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);
SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedSum(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));

SELECT number AS value, number AS time, exponentialTimeDecayedSum(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 10, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialTimeDecayedSum(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 1, 50) AS bar
FROM
(
    SELECT
        (number % 5) = 0 AS value,
        number AS time,
        exponentialTimeDecayedSum(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

-- exponentialTimeDecayedMax
SELECT number = 0 AS value, number AS time, exponentialTimeDecayedMax(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);
SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedMax(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));

SELECT number AS value, number AS time, exponentialTimeDecayedMax(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 10, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialTimeDecayedMax(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 1, 50) AS bar
FROM
(
    SELECT
        (number % 5) = 0 AS value,
        number AS time,
        exponentialTimeDecayedMax(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

-- exponentialTimeDecayedCount
SELECT number = 0 AS value, number AS time, exponentialTimeDecayedCount(1)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);
SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedCount(10)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));

SELECT number AS value, number AS time, exponentialTimeDecayedCount(1)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 10, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialTimeDecayedCount(5)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 20, 50) AS bar
FROM
(
    SELECT
        (number % 5) = 0 AS value,
        number AS time,
        exponentialTimeDecayedCount(10)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

-- exponentialTimeDecayedAvg
SELECT number = 0 AS value, number AS time, exponentialTimeDecayedAvg(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);
SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedAvg(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));

SELECT number AS value, number AS time, exponentialTimeDecayedAvg(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 5, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialTimeDecayedAvg(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);

SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 0.5, 50) AS bar
FROM
(
    SELECT
        (number % 5) = 0 AS value,
        number AS time,
        exponentialTimeDecayedAvg(100)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);
