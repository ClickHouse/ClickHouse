SELECT n, k FROM (SELECT number AS n, toFixedString(materialize('   '), 3) AS k FROM system.numbers LIMIT 100000) GROUP BY n, k ORDER BY n DESC, k LIMIT 10;
