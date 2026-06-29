SET max_memory_usage = 10000000000;

-- Unneeded column is removed from subquery.
SELECT count() FROM (SELECT number, groupArray(repeat(toString(number), 1000000)) FROM numbers(1000000) GROUP BY number);
-- Unneeded column cannot be removed from subquery and the query is out of memory
SELECT count() FROM (SELECT number, groupArray(repeat(toString(number), 1000000)) AS agg FROM numbers(1000000) GROUP BY number HAVING notEmpty(agg)); -- { serverError MEMORY_LIMIT_EXCEEDED }
