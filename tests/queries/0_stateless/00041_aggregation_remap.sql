SELECT number, count() FROM (SELECT number FROM system.numbers LIMIT 200000) GROUP BY number ORDER BY count(), number LIMIT 10
