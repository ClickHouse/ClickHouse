SELECT 'Merge/State: window states merged in aggregate() (non-window consumes window states)';
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

SELECT 'If: window states merged in aggregate() (non-window consumes window states)';
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

SELECT 'Array: window states merged in aggregate() (non-window consumes window states)';
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

SELECT 'Distinct: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVDistinct(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVDistinctMerge(st)
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
                    cramersVDistinctState(toUInt8(number % 10), toUInt8(number % 6))
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

SELECT 'ForEach: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVForEach(array(toUInt8(number % 10)), array(toUInt8(number % 6)))[1] FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVForEachMerge(st)[1]
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
                    cramersVForEachState(array(toUInt8(number % 10)), array(toUInt8(number % 6)))
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

SELECT 'ArgMin: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVArgMin(toUInt8(number % 10), toUInt8(number % 6), number % 2) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVArgMinMerge(st)
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
                    cramersVArgMinState(toUInt8(number % 10), toUInt8(number % 6), number % 2)
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

SELECT 'ArgMax: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVArgMax(toUInt8(number % 10), toUInt8(number % 6), number % 2) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVArgMaxMerge(st)
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
                    cramersVArgMaxState(toUInt8(number % 10), toUInt8(number % 6), number % 2)
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

SELECT 'OrNull: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVOrNull(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVOrNullMerge(st)
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
                    cramersVOrNullState(toUInt8(number % 10), toUInt8(number % 6))
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

SELECT 'OrDefault: window states merged in aggregate() (non-window consumes window states)';
WITH
    (SELECT cramersVOrDefault(toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(10_000)) AS direct_raw,
    (
        SELECT cramersVOrDefaultMerge(st)
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
                    cramersVOrDefaultState(toUInt8(number % 10), toUInt8(number % 6))
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

SELECT 'Null (internal): window states merged in aggregate() (non-window consumes window states)';
WITH
    (
        SELECT cramersV(toNullable(toUInt8(number % 10)), toNullable(toUInt8(number % 6)))
        FROM numbers(10_000)
    ) AS direct_raw,
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
                    cramersVState(toNullable(toUInt8(number % 10)), toNullable(toUInt8(number % 6)))
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

SELECT 'Resample: window states merged in aggregate() (non-window consumes window states)';
WITH
    (
        SELECT cramersVResample(0, 10000, 1000)(toUInt8(number % 10), toUInt8(number % 6), number)
        FROM numbers(10_000)
    ) AS direct_raw,
    (
        SELECT cramersVResampleMerge(0, 10000, 1000)(st)
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
                    cramersVResampleState(0, 10000, 1000)(toUInt8(number % 10), toUInt8(number % 6), number)
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
    arrayMap(x -> round(x, 4), direct_raw) AS direct,
    arrayMap(x -> round(x, 4), merged_raw) AS merged,
    arrayAll(z -> abs(z.1 - z.2) < 1e-9, arrayZip(direct_raw, merged_raw)) AS ok;
