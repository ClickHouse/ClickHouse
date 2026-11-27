set enable_analyzer = 1;

EXPLAIN QUERY TREE dump_ast = 1
SELECT a
FROM numbers(10)
GROUP BY (number > 10) OR ((number > 2) AND (number > 10)) AS a
HAVING a
SETTINGS optimize_extract_common_expressions = 0;

SELECT a
FROM numbers(10)
GROUP BY (number > 10) OR ((number > 2) AND (number > 10)) AS a
HAVING a
SETTINGS optimize_extract_common_expressions = 0;

EXPLAIN QUERY TREE dump_ast = 1
SELECT a
FROM numbers(10)
GROUP BY (number > 10) OR ((number > 2) AND (number > 10)) AS a
HAVING a
SETTINGS optimize_extract_common_expressions = 1;

SELECT a
FROM numbers(10)
GROUP BY (number > 10) OR ((number > 2) AND (number > 10)) AS a
HAVING a
SETTINGS optimize_extract_common_expressions = 1;
