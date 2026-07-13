SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT
    t.a,
    u.a
FROM
    (
        SELECT
            1 AS a
    ) AS t,
    (
        SELECT 1 AS a
        QUALIFY 0 = (t.a AS alias668)
    ) AS u; -- { serverError NOT_IMPLEMENTED }

SELECT
    t.a,
    u.a
FROM
    (
        SELECT
            1 AS a
    ) AS t,
    (
        SELECT
            DISTINCT *,
            *,
            27,
            '======Before Truncate======',
            materialize(27),
            27,
            *,
            isZeroOrNull(27),
            27,
            materialize(27),
            *,
            * IS NOT NULL,
            *,
            27,
            *,
            toFixedString('======Before Truncate======', 27),
            27,
            27,
            27,
            27,
            toLowCardinality(27),
            toNullable(materialize(27)),
            * IS NOT NULL,
            1 AS a QUALIFY (
                (
                    *,
                    27,
                    materialize(27),
                    27,
                    '======Before Truncate======',
                    27,
                    27,
                    (27 IS NOT NULL),
                    * IS NOT NULL
                ) IS NULL
            ) = (t.a AS alias668)
    ) AS u; -- { serverError NOT_IMPLEMENTED }

SELECT
    c,
    a c
FROM
    (
        SELECT
            1 a
    ) X
    CROSS JOIN (
        SELECT
            1
        WHERE
            c = 1
    ) Y
    CROSS JOIN (
        SELECT
            1 c
    ) Z; -- { serverError NOT_IMPLEMENTED }
