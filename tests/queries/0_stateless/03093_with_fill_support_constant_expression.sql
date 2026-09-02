WITH 0 AS l, 10 AS r SELECT number * 2 FROM numbers(5) ORDER BY 1 WITH FILL FROM l TO r;
WITH 0 AS l, 10 AS r SELECT number * 2 FROM numbers(5) ORDER BY 1 WITH FILL FROM l TO l + r;
