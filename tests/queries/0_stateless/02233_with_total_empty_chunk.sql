SELECT (NULL, NULL, NULL, NULL, NULL, NULL, NULL) FROM numbers(0) GROUP BY number WITH TOTALS HAVING sum(number) <= arrayJoin([]);
