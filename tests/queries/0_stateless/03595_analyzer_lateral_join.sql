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
