-- Tags: long, no-fasttest

-- The result slightly differs but it's ok since `uniqueTheta` is an approximate function.
set max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'uniqTheta';

SELECT Y, uniqTheta(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqTheta round(float)';

SELECT Y, uniqTheta(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqTheta round(toFloat32())';

SELECT Y, uniqTheta(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqTheta IPv4NumToString';

SELECT Y, uniqTheta(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqTheta(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqTheta remote()';

SELECT uniqTheta(dummy) FROM remote('127.0.0.{2,3}', system.one);


SELECT 'uniqTheta precise';
SELECT uniqExact(number) FROM numbers(1e7);
SELECT uniqCombined(number) FROM numbers(1e7);
SELECT uniqCombined64(number) FROM numbers(1e7);
SELECT uniqTheta(number) FROM numbers(1e7);

