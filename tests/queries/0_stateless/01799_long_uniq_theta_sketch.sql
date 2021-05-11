SELECT 'uniqThetaSketch';

SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT 'uniqThetaSketch round(float)';

SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT 'uniqThetaSketch round(toFloat32())';

SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqThetaSketch(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT 'uniqThetaSketch IPv4NumToString';

SELECT Y, uniqThetaSketch(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqThetaSketch(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqThetaSketch(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT 'uniqThetaSketch remote()';

SELECT uniqThetaSketch(dummy) FROM remote('127.0.0.{2,3}', system.one);


SELECT 'uniqThetaSketch precise';
SELECT uniqExact(number) FROM numbers(1e7);
SELECT uniqCombined(number) FROM numbers(1e7);
SELECT uniqCombined64(number) FROM numbers(1e7);
SELECT uniqThetaSketch(number) FROM numbers(1e7);

