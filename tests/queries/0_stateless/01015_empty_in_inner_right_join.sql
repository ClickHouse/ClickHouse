SET joined_subquery_requires_alias = 0;

SELECT 'IN empty set',count() FROM system.numbers WHERE number IN (SELECT toUInt64(1) WHERE 0);
SELECT 'IN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 WHERE t1.number IN (SELECT toUInt64(1) WHERE 1);
SELECT 'NOT IN empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) WHERE number NOT IN (SELECT toUInt64(1) WHERE 0);

SELECT 'INNER JOIN empty set',count() FROM system.numbers INNER JOIN (SELECT toUInt64(1) AS x WHERE 0) ON system.numbers.number = x;
SELECT 'INNER JOIN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 INNER JOIN (SELECT toUInt64(1) AS x WHERE 1) ON t1.number = x;

SELECT 'RIGHT JOIN empty set',count() FROM system.numbers RIGHT JOIN (SELECT toUInt64(1) AS x WHERE 0) ON system.numbers.number = x;
SELECT 'RIGHT JOIN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 RIGHT JOIN (SELECT toUInt64(1) AS x WHERE 1) ON t1.number = x;

SELECT 'LEFT JOIN empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 LEFT JOIN (SELECT toUInt64(1) AS x WHERE 0) ON t1.number = x;
SELECT 'LEFT JOIN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 LEFT JOIN (SELECT toUInt64(1) AS x WHERE 1) ON t1.number = x;

SELECT 'multiple sets IN empty set OR IN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) WHERE number IN (SELECT toUInt64(1) WHERE 0) OR number IN (SELECT toUInt64(1) WHERE 1);
SELECT 'multiple sets IN empty set OR NOT IN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) WHERE number IN (SELECT toUInt64(1) WHERE 0) OR number NOT IN (SELECT toUInt64(1) WHERE 1);
SELECT 'multiple sets NOT IN empty set AND IN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) WHERE number NOT IN (SELECT toUInt64(1) WHERE 0) AND number IN (SELECT toUInt64(1) WHERE 1);
SELECT 'multiple sets INNER JOIN empty set AND IN empty set',count() FROM system.numbers INNER JOIN (SELECT toUInt64(1) AS x WHERE 0) ON system.numbers.number = x WHERE system.numbers.number IN (SELECT toUInt64(1) WHERE 0);
SELECT 'multiple sets INNER JOIN empty set AND IN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 INNER JOIN (SELECT toUInt64(1) AS x WHERE 0) ON t1.number = x WHERE t1.number IN (SELECT toUInt64(1) WHERE 1);
SELECT 'multiple sets INNER JOIN non-empty set AND IN non-empty set',count() FROM (SELECT number FROM system.numbers LIMIT 10) t1 INNER JOIN (SELECT toUInt64(1) AS x WHERE 1) ON t1.number = x WHERE t1.number IN (SELECT toUInt64(1) WHERE 1);

SELECT 'IN empty set equals 0', count() FROM numbers(10) WHERE (number IN (SELECT toUInt64(1) WHERE 0)) = 0;
SELECT 'IN empty set sum if', sum(if(number IN (SELECT toUInt64(1) WHERE 0), 2, 1)) FROM numbers(10);
