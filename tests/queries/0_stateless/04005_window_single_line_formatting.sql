-- Test that WINDOW clause formats correctly in single-line mode.
-- This is a regression test for a bug where formatImplMultiline was called
-- unconditionally for WINDOW, inserting newlines even in single-line mode.

-- Basic WINDOW clause single-line formatting
SELECT formatQuerySingleLine('SELECT 1 WINDOW w0 AS (PARTITION BY 1 ORDER BY 1)');

-- Multiple WINDOW definitions
SELECT formatQuerySingleLine('SELECT 1 WINDOW w0 AS (PARTITION BY 1 ORDER BY 1), w1 AS (ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)');

-- Round-trip consistency: formatQuerySingleLine should be idempotent
SELECT formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q) AS is_consistent
FROM (SELECT 'SELECT 1 FROM numbers(10) GROUP BY ALL WINDOW w0 AS (PARTITION BY 1 ORDER BY 1), w1 AS (ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w2 AS (PARTITION BY 1, 1 ORDER BY 1 DESC NULLS FIRST ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)' AS q);

-- WINDOW inside compound set operations with INSERT
SELECT formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q) AS is_consistent
FROM (SELECT 'INSERT INTO FUNCTION url(''test'', ''PrettyCompact'', ''c0 UInt32'') ((SELECT 1 FROM numbers(10) GROUP BY 1 WITH ROLLUP ORDER BY 1 ASC) EXCEPT (SELECT 1 FROM numbers(10) GROUP BY ALL WINDOW w0 AS (PARTITION BY 1 ORDER BY 1 ASC), w1 AS (ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) LIMIT 1, 10 SETTINGS min_count_to_compile_sort_description = 3))' AS q);
