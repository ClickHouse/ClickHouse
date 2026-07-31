SELECT 'cramersV: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(cramersV, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'cramersV: merge roundtrip';
WITH
    (SELECT cramersV(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(cramersVState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(cramersV, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'cramersV: OVER(full frame) matches aggregate';
WITH
    (SELECT cramersV(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersV(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    ) AS over_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_raw, 4) AS over,
    abs(direct_raw - over_raw) < 1e-9;


SELECT 'cramersV: window state -> normal deserialize (serialization compat)';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st_win) AS direct,
        finalizeAggregation(CAST(CAST(st_win, 'String'), 'AggregateFunction(cramersV, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS st_win
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    )
);


SELECT 'cramersV: normal states merged in OVER() (window consumes normal states)';
WITH
    (SELECT cramersV(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVMerge(st)
            OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM
        (
            SELECT
                number % 1000 AS g,
                CAST(
                    CAST(cramersVState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(cramersV, UInt8, UInt8)'
                ) AS st
            FROM numbers(10_000)
            GROUP BY g
            ORDER BY g
        )
        ORDER BY g DESC
        LIMIT 1
    ) AS over_merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_merged_raw, 4) AS merged_over,
    abs(direct_raw - over_merged_raw) < 1e-9;


SELECT 'cramersV: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersV(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    cramersVState(toUInt8(number % 10), toUInt8(number % 6))
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'cramersVIf: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVIf(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVIfMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    cramersVIfState(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5)
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'cramersVArray: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVArray(array(toUInt8(number % 10)), array(toUInt8(number % 6))) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVArrayMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    cramersVArrayState(array(toUInt8(number % 10)), array(toUInt8(number % 6)))
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'cramersVStateArray: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT finalizeAggregation(cramersVStateArray(array(toUInt8(number % 10)), array(toUInt8(number % 6)))) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT finalizeAggregation(cramersVStateArrayMerge(st))
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    cramersVStateArrayState(array(toUInt8(number % 10)), array(toUInt8(number % 6)))
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'cramersVBiasCorrected: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'cramersVBiasCorrected: merge roundtrip';
WITH
    (SELECT cramersVBiasCorrected(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVBiasCorrectedMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'cramersVBiasCorrected: OVER(full frame) matches aggregate';
WITH
    (SELECT cramersVBiasCorrected(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVBiasCorrected(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    ) AS over_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_raw, 4) AS over,
    abs(direct_raw - over_raw) < 1e-9;


SELECT 'cramersVBiasCorrected: window state -> normal deserialize (serialization compat)';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st_win) AS direct,
        finalizeAggregation(CAST(CAST(st_win, 'String'), 'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS st_win
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    )
);


SELECT 'cramersVBiasCorrected: normal states merged in OVER() (window consumes normal states)';
WITH
    (SELECT cramersVBiasCorrected(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVBiasCorrectedMerge(st)
            OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM
        (
            SELECT
                number % 1000 AS g,
                CAST(
                    CAST(cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)'
                ) AS st
            FROM numbers(10_000)
            GROUP BY g
            ORDER BY g
        )
        ORDER BY g DESC
        LIMIT 1
    ) AS over_merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_merged_raw, 4) AS merged_over,
    abs(direct_raw - over_merged_raw) < 1e-9;


SELECT 'cramersVBiasCorrected: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVBiasCorrected(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVBiasCorrectedMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6))
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'cramersVBiasCorrectedIf: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVBiasCorrectedIf(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVBiasCorrectedIfMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    cramersVBiasCorrectedIfState(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5)
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'theilsU: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(theilsU, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'theilsU: merge roundtrip';
WITH
    (SELECT theilsU(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT theilsUMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(theilsUState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(theilsU, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'theilsU: OVER(full frame) matches aggregate';
WITH
    (SELECT theilsU(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT theilsU(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    ) AS over_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_raw, 4) AS over,
    abs(direct_raw - over_raw) < 1e-9;


SELECT 'theilsU: window state -> normal deserialize (serialization compat)';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st_win) AS direct,
        finalizeAggregation(CAST(CAST(st_win, 'String'), 'AggregateFunction(theilsU, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS st_win
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    )
);


SELECT 'theilsU: normal states merged in OVER() (window consumes normal states)';
WITH
    (SELECT theilsU(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT theilsUMerge(st)
            OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM
        (
            SELECT
                number % 1000 AS g,
                CAST(
                    CAST(theilsUState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(theilsU, UInt8, UInt8)'
                ) AS st
            FROM numbers(10_000)
            GROUP BY g
            ORDER BY g
        )
        ORDER BY g DESC
        LIMIT 1
    ) AS over_merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_merged_raw, 4) AS merged_over,
    abs(direct_raw - over_merged_raw) < 1e-9;


SELECT 'theilsU: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT theilsU(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT theilsUMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    theilsUState(toUInt8(number % 10), toUInt8(number % 6))
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'theilsUIf: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT theilsUIf(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT theilsUIfMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    theilsUIfState(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5)
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'contingency: state roundtrip';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st) AS direct,
        finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(contingency, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) AS st
        FROM numbers(10_000)
    )
);

SELECT 'contingency: merge roundtrip';
WITH
    (SELECT contingency(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyMerge(st2)
        FROM
        (
            SELECT
                CAST(
                    CAST(contingencyState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(contingency, UInt8, UInt8)'
                ) AS st2
            FROM numbers(10_000)
            GROUP BY number % 1000
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;

SELECT 'contingency: OVER(full frame) matches aggregate';
WITH
    (SELECT contingency(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingency(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    ) AS over_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_raw, 4) AS over,
    abs(direct_raw - over_raw) < 1e-9;


SELECT 'contingency: window state -> normal deserialize (serialization compat)';
SELECT
    round(direct, 4) AS direct,
    round(roundtrip, 4) AS roundtrip,
    abs(direct - roundtrip) < 1e-9
FROM
(
    SELECT
        finalizeAggregation(st_win) AS direct,
        finalizeAggregation(CAST(CAST(st_win, 'String'), 'AggregateFunction(contingency, UInt8, UInt8)')) AS roundtrip
    FROM
    (
        SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6))
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS st_win
        FROM numbers(10_000)
        ORDER BY number DESC
        LIMIT 1
    )
);

SELECT 'contingency: normal states merged in OVER() (window consumes normal states)';
WITH
    (SELECT contingency(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyMerge(st)
            OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM
        (
            SELECT
                number % 1000 AS g,
                CAST(
                    CAST(contingencyState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(contingency, UInt8, UInt8)'
                ) AS st
            FROM numbers(10_000)
            GROUP BY g
            ORDER BY g
        )
        ORDER BY g DESC
        LIMIT 1
    ) AS over_merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_merged_raw, 4) AS merged_over,
    abs(direct_raw - over_merged_raw) < 1e-9;


SELECT 'contingency: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT contingency(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    contingencyState(toUInt8(number % 10), toUInt8(number % 6))
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'contingencyIf: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT contingencyIf(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyIfMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    contingencyIfState(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5)
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9;


SELECT 'Reject different state types in window functions';
SELECT cramersVMerge(theilsUState(toUInt8(1), toUInt8(1))) SETTINGS enable_analyzer = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cramersVMerge(theilsUState(toUInt8(1), toUInt8(1))) SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_AGGREGATION }

WITH
    (SELECT theilsUIf(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyIfMerge(st)
        FROM
        (
            SELECT
                g,
                st_win AS st
            FROM
            (
                SELECT
                    number,
                    number % 1000 AS g,
                    theilsUIfState(toUInt8(number % 10), toUInt8(number % 6), number % 10 < 5)
                        OVER (
                            PARTITION BY (number % 1000)
                            ORDER BY number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS st_win
                FROM numbers(10_000)
            )
            ORDER BY g, number
            LIMIT 1 BY g
        )
    ) AS merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(merged_raw, 4) AS merged,
    abs(direct_raw - merged_raw) < 1e-9; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH
    (SELECT contingency(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT contingencyMerge(st)
            OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        FROM
        (
            SELECT
                number % 1000 AS g,
                CAST(
                    CAST(theilsUState(toUInt8(number % 10), toUInt8(number % 6)), 'String'),
                    'AggregateFunction(theilsU, UInt8, UInt8)'
                ) AS st
            FROM numbers(10_000)
            GROUP BY g
            ORDER BY g
        )
        ORDER BY g DESC
        LIMIT 1
    ) AS over_merged_raw
SELECT
    round(direct_raw, 4) AS direct,
    round(over_merged_raw, 4) AS merged_over,
    abs(direct_raw - over_merged_raw) < 1e-9;  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
