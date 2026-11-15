SELECT 'With Partition By';

SELECT
    round(exp(number), 3) AS x,
    percent_rank(x) OVER (ORDER BY number ASC) AS rank,
    ntile(10) OVER (PARTITION BY 1 ORDER BY number ASC) AS bucket
FROM numbers(11);

SELECT 'No Partition By';

SELECT
    round(exp(number), 3) AS x,
    percent_rank(x) OVER (ORDER BY number ASC) AS rank,
    ntile(10) OVER (ORDER BY number ASC) AS bucket
FROM numbers(11);
