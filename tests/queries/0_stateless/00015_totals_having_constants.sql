SELECT number, count() / 0.1 FROM (SELECT number FROM system.numbers LIMIT 10) GROUP BY number WITH TOTALS HAVING count() > 0.1 ORDER BY number
