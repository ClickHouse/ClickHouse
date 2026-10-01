-- The bare form with actions is highlighted like the parenthesized form.
SELECT highlightQuery('EXPLAIN TEXT SELECT 1 AS x ONELINE');
SELECT highlightQuery('EXPLAIN TEXT (SELECT 1 AS x) ONELINE');
SELECT highlightQuery('EXPLAIN TEXT SELECT ''a%'' LIKE ''b%'' LIMIT 5 MODIFY LIMIT 2, ONELINE FORMAT JSON');
SELECT highlightQuery('EXPLAIN TEXT (SELECT ''a%'' LIKE ''b%'' LIMIT 5) MODIFY LIMIT 2, ONELINE FORMAT JSON');
