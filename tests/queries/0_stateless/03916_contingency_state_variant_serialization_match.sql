WITH src AS
(
    SELECT
        number % 17 AS a,
        number % 23 AS b
    FROM numbers(1000)
)
SELECT
    (SELECT hex(theilsUState(a, b)) FROM src) = (SELECT s FROM (SELECT hex(theilsUState(a, b) OVER ()) AS s FROM src LIMIT 1)),
    (SELECT hex(cramersVState(a, b)) FROM src) = (SELECT s FROM (SELECT hex(cramersVState(a, b) OVER ()) AS s FROM src LIMIT 1)),
    (SELECT hex(cramersVBiasCorrectedState(a, b)) FROM src) = (SELECT s FROM (SELECT hex(cramersVBiasCorrectedState(a, b) OVER ()) AS s FROM src LIMIT 1)),
    (SELECT hex(contingencyState(a, b)) FROM src) = (SELECT s FROM (SELECT hex(contingencyState(a, b) OVER ()) AS s FROM src LIMIT 1));


WITH src AS
(
    SELECT
        intDiv(number, 25) AS grp,
        number AS ord,
        number % 17 AS a,
        number % 23 AS b
    FROM numbers(100)
)
SELECT
    agg.grp,
    agg.theils_state = win.theils_state,
    agg.cramersV_state = win.cramersV_state,
    agg.cramersVBiasCorrected_state = win.cramersVBiasCorrected_state,
    agg.contingency_state = win.contingency_state
FROM
(
    SELECT
        grp,
        hex(theilsUState(a, b)) AS theils_state,
        hex(cramersVState(a, b)) AS cramersV_state,
        hex(cramersVBiasCorrectedState(a, b)) AS cramersVBiasCorrected_state,
        hex(contingencyState(a, b)) AS contingency_state
    FROM src
    GROUP BY grp
) AS agg
INNER JOIN
(
    SELECT DISTINCT
        grp,
        hex(theilsUState(a, b) OVER w) AS theils_state,
        hex(cramersVState(a, b) OVER w) AS cramersV_state,
        hex(cramersVBiasCorrectedState(a, b) OVER w) AS cramersVBiasCorrected_state,
        hex(contingencyState(a, b) OVER w) AS contingency_state
    FROM src
    WINDOW w AS (PARTITION BY grp ORDER BY ord ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
) AS win USING (grp)
ORDER BY grp;
