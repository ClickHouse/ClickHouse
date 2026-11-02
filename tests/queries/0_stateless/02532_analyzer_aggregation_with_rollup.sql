SET enable_analyzer = 1;

SELECT
    sum(a.number) AS total,
    c.number AS cn,
    b.number AS bn,
    grouping(c.number) + grouping(b.number) AS l,
    rank() OVER (PARTITION BY grouping(c.number) + grouping(b.number), multiIf(grouping(c.number) = 0, b.number, NULL) ORDER BY sum(a.number) DESC) AS r
FROM numbers(10) AS a, numbers(10) AS b, numbers(10) AS c
GROUP BY
    cn,
    bn
    WITH ROLLUP
ORDER BY
    total ASC,
    cn ASC,
    bn ASC,
    l ASC,
    r ASC
LIMIT 10;
