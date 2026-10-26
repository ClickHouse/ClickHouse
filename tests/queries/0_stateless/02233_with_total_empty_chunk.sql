SET enable_analyzer = 1;

SELECT (NULL, NULL, NULL, NULL, NULL, NULL, NULL) FROM numbers(0) GROUP BY number WITH TOTALS HAVING sum(number) <= arrayJoin([]) -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER };
