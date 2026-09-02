SELECT materialize('') AS k1, number % 123 AS k2, count() AS c FROM (SELECT * FROM system.numbers LIMIT 1000) GROUP BY k1, k2 ORDER BY k1, k2;
