SET allow_experimental_analyzer = 1;

-- Basic functionality with GROUPING and GROUP BY ALL WITH ROLLUP
SELECT l.number, sum(r.number), grouping(l.number)
FROM numbers(1) l JOIN numbers(2) r ON l.number < r.number
GROUP BY ALL WITH ROLLUP;

-- Multiple GROUPING functions
SELECT l.number, r.number % 3 AS mod3, sum(r.number),
       grouping(l.number), grouping(mod3), grouping(l.number, mod3)
FROM numbers(1) l JOIN numbers(2) r ON l.number < r.number
GROUP BY ALL WITH ROLLUP;

-- GROUPING with CUBE
SELECT l.number, r.number % 3 AS mod3, sum(r.number),
       grouping(l.number), grouping(mod3)
FROM numbers(1) l JOIN numbers(2) r ON l.number < r.number
GROUP BY ALL WITH CUBE;

-- Mix of regular columns and expressions with GROUPING
SELECT
    l.number,
    l.number % 2 AS parity,
    sum(r.number),
    max(r.number),
    grouping(l.number),
    grouping(parity)
FROM numbers(1) l JOIN numbers(2) r ON l.number < r.number
GROUP BY ALL WITH ROLLUP;

-- Verify error is still thrown when GROUPING is explicitly in GROUP BY
SELECT l.number, sum(r.number), grouping(l.number) as g
FROM numbers(1) l JOIN numbers(2) r ON l.number < r.number
GROUP BY l.number, g WITH ROLLUP;  -- { serverError ILLEGAL_AGGREGATION }
