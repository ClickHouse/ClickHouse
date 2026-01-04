SELECT 'cramersVWindow: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(cramersVWindow, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT cramersVWindowState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'cramersVWindow: merge roundtrip';
WITH
    (SELECT cramersVWindow(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVWindowMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(cramersVWindowState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(cramersVWindow, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'cramersVBiasCorrectedWindow: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(cramersVBiasCorrectedWindow, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT cramersVBiasCorrectedWindowState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'cramersVBiasCorrectedWindow: merge roundtrip';
WITH
    (SELECT cramersVBiasCorrectedWindow(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVBiasCorrectedWindowMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(cramersVBiasCorrectedWindowState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(cramersVBiasCorrectedWindow, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'theilsUWindow: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(theilsUWindow, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT theilsUWindowState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'theilsUWindow: merge roundtrip';
WITH
    (SELECT theilsUWindow(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT theilsUWindowMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(theilsUWindowState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(theilsUWindow, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'contingencyWindow: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(contingencyWindow, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT contingencyWindowState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'contingencyWindow: merge roundtrip';
WITH
    (SELECT contingencyWindow(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyWindowMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(contingencyWindowState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(contingencyWindow, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;
