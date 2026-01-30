SELECT 'Sanity Check to ensure window variant is choosen. Non-window variant would take too long';
SELECT
    number,
    round(cv, 2),
    round(cvc, 2),
    round(tu, 2),
    round(ct, 2)
FROM
(
  SELECT number,
      cramersV(number, number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 99999 FOLLOWING) AS cv,
      cramersVBiasCorrected(number % 99999, number % 99999) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 99999 FOLLOWING) AS cvc,
      theilsU(number, number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 99999 FOLLOWING) AS tu,
      contingency(number, number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 99999 FOLLOWING) AS ct
  FROM numbers(100000)
)
ORDER BY number
LIMIT 1;

SELECT 'OVER';
SELECT
    round(cramersV(a,b) OVER (), 2),
    round(cramersVBiasCorrected(a,b) OVER (), 2),
    round(theilsU(a,b) OVER (), 2),
    round(theilsU(b,a) OVER (), 2),
    round(contingency(a,b) OVER (), 2)
FROM (
    SELECT number, number % 3 AS a, number % 5 AS b
    FROM numbers(10)
)
ORDER BY number;

SELECT 'OVER with PARTITION BY';
SELECT
    grp,
    round(cramersV(a,b) OVER (PARTITION BY grp), 2),
    round(cramersVBiasCorrected(a,b) OVER (PARTITION BY grp), 2),
    round(theilsU(a,b) OVER (PARTITION BY grp), 2),
    round(theilsU(b,a) OVER (PARTITION BY grp), 2),
    round(contingency(a,b) OVER (PARTITION BY grp), 2)
FROM (
    SELECT
        number % 4 AS grp,
        number % 3 AS a,
        number AS b
    FROM numbers(15)
)
ORDER BY grp;

SELECT 'OVER with ORDER BY';
SELECT
    number,
    round(cramersV(a,b) OVER (ORDER BY number), 2),
    round(cramersVBiasCorrected(a,b) OVER (ORDER BY number), 2),
    round(theilsU(a,b) OVER (ORDER BY number), 2),
    round(theilsU(b,a) OVER (ORDER BY number), 2),
    round(contingency(a,b) OVER (ORDER BY number), 2)
FROM (
    SELECT number, number % 3 AS a, number % 7 AS b
    FROM numbers(20)
)
ORDER BY number;

SELECT 'OVER with ORDER BY with ROWS BETWEEN';
SELECT
    number,
    round(cramersV(a,b) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING), 2),
    round(cramersVBiasCorrected(a,b) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING), 2),
    round(theilsU(a,b) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING), 2),
    round(theilsU(b,a) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING), 2),
    round(contingency(a,b) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING), 2)
FROM (
    SELECT number, number % 3 AS a, number % 7 AS b
    FROM numbers(20)
)
ORDER BY number;
