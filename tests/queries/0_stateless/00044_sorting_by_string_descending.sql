SELECT s FROM (SELECT materialize('abc') AS s FROM system.numbers LIMIT 100) ORDER BY s DESC
