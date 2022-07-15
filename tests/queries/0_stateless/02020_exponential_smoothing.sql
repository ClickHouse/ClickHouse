SELECT 'exponentialMovingAverage';

SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialMovingAverage(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));
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

SELECT 'exponentialTimeDecayedSum';

SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedSum(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));
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

SELECT 'exponentialTimeDecayedMax';

SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedMax(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));
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

SELECT 'exponentialTimeDecayedCount';

SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedCount(1)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));
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

SELECT 'exponentialTimeDecayedAvg';

SELECT value, time, round(exp_smooth, 3) FROM (SELECT number = 0 AS value, number AS time, exponentialTimeDecayedAvg(1)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth FROM numbers(10));
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

SELECT 'Check `exponentialTimeDecayed.*` supports sliding windows';

SELECT
    x,
    t,
    round(sum, 12),
    round(max, 12),
    round(count, 12),
    round(avg, 12)
FROM
(
    SELECT
        d[1] AS x,
        d[2] AS t,
        exponentialTimeDecayedSum(100)(x, t) OVER w AS sum,
        exponentialTimeDecayedMax(100)(x, t) OVER w AS max,
        exponentialTimeDecayedCount(100)(t) OVER w AS count,
        exponentialTimeDecayedAvg(100)(x, t) OVER w AS avg
    FROM
    (
        SELECT [[2, 1], [1, 2], [0, 3], [4, 4], [5, 5], [1, 6], [0, 7], [10, 8]] AS d
    )
    ARRAY JOIN d
    WINDOW w AS (ORDER BY 1 ASC Rows BETWEEN 2 PRECEDING AND 2 FOLLOWING)
);

SELECT
    x,
    t,
    round(sum, 12),
    round(max, 12),
    round(count, 12),
    round(avg, 12)
FROM
(
    SELECT
        sin(number) AS x,
        number AS t,
        exponentialTimeDecayedSum(100)(x, t) OVER w AS sum,
        exponentialTimeDecayedMax(100)(x, t) OVER w AS max,
        exponentialTimeDecayedCount(100)(t) OVER w AS count,
        exponentialTimeDecayedAvg(100)(x, t) OVER w AS avg
    FROM numbers(1000000)
    WINDOW w AS (ORDER BY 1 ASC Rows BETWEEN 2 PRECEDING AND 2 FOLLOWING)
)
FORMAT `Null`;

SELECT 'Check `exponentialTimeDecayedMax` works with negative values';

SELECT
    x,
    t,
    round(max, 12)
FROM
(
    SELECT
        d[1] AS x,
        d[2] AS t,
        exponentialTimeDecayedMax(100)(-x, t) OVER w AS max
    FROM
    (
        SELECT [[2, 1], [1, 2], [10, 3], [4, 4], [5, 5], [1, 6], [10, 7], [10, 8], [10, 9], [9.81, 10], [9.9, 11]] AS d
    )
    ARRAY JOIN d
    WINDOW w AS (ORDER BY 1 ASC Rows BETWEEN 2 PRECEDING AND 2 FOLLOWING)
);
