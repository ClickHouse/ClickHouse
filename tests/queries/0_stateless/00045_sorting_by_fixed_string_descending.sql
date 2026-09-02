SELECT s FROM (SELECT toFixedString(materialize('abc'), 3) AS s FROM system.numbers LIMIT 100) ORDER BY s DESC
