SET enable_debug_queries = 1;
SET optimize_duplicate_order_by_and_distinct = 1;

ANALYZE SELECT DISTINCT number FROM numbers(1);
ANALYZE SELECT DISTINCT number FROM (SELECT DISTINCT number FROM numbers(1));
ANALYZE SELECT DISTINCT number * 2 FROM (SELECT DISTINCT number * 2, number FROM numbers(1));
ANALYZE SELECT DISTINCT number FROM (SELECT DISTINCT number * 2 AS number FROM numbers(1));
ANALYZE SELECT DISTINCT b, a FROM (SELECT DISTINCT number % 2 AS a, number % 3 AS b FROM numbers(100));
ANALYZE SELECT DISTINCT a FROM (SELECT DISTINCT number % 2 AS a, number % 3 AS b FROM numbers(100));
ANALYZE SELECT DISTINCT a FROM (SELECT DISTINCT a FROM (SELECT DISTINCT number % 2 AS a, number % 3 AS b FROM numbers(100)));
ANALYZE SELECT DISTINCT a FROM (SELECT DISTINCT a, b FROM (SELECT DISTINCT number % 2 AS a, number % 3 AS b FROM numbers(100)));
ANALYZE SELECT DISTINCT a, b FROM (SELECT DISTINCT b, a FROM (SELECT DISTINCT number a, number b FROM numbers(1)));
ANALYZE SELECT DISTINCT a, b FROM (SELECT b, a, a + b FROM (SELECT DISTINCT number % 2 AS a, number % 3 AS b FROM numbers(100)));
ANALYZE SELECT DISTINCT a FROM (SELECT a FROM (SELECT DISTINCT number % 2 AS a, number % 3 AS b FROM numbers(100)));
ANALYZE SELECT DISTINCT number FROM (SELECT DISTINCT number FROM numbers(1)) t1 CROSS JOIN numbers(2) t2;
ANALYZE SELECT DISTINCT number FROM (SELECT DISTINCT number FROM numbers(1) t1 CROSS JOIN numbers(2) t2);

ANALYZE SELECT DISTINCT number FROM
(
    (SELECT DISTINCT number FROM numbers(1))
    UNION ALL
    (SELECT DISTINCT number FROM numbers(2))
);

--

SELECT DISTINCT number FROM
(
    (SELECT DISTINCT number FROM numbers(1))
    UNION ALL
    (SELECT DISTINCT number FROM numbers(2))
)
ORDER BY number;
