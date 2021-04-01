SELECT * FROM numbers(4) GROUP BY number WITH TOTALS HAVING sum(number) <= arrayJoin([]); -- { serverError 44 }
SELECT * FROM numbers(4) GROUP BY number WITH TOTALS HAVING sum(number) <= arrayJoin([3, 2, 1, 0]) ORDER BY number; -- { serverError 44 }
