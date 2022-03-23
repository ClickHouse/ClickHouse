SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 1 SECOND) AS nnd
FROM (
    SELECT * FROM VALUES (
        'ts DateTime64(3, "UTC"), metric Int32',
        (toDateTime64('1979-12-12 21:21:21.123', 3, 'UTC'), 1),
        (toDateTime64('1979-12-12 21:21:21.124', 3, 'UTC'), 2),
        (toDateTime64('1979-12-12 21:21:21.127', 3, 'UTC'), 3),
        (toDateTime64('1979-12-12 21:21:21.129', 3, 'UTC'), 2),
        (toDateTime('1979-12-12 21:21:22', 'UTC'), 13)
        )
    );