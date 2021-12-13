-- uniqHLL12

SELECT 'uniqHLL12';

SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqHLL12 round(float)';

SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqHLL12 round(toFloat32())';

SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqHLL12 IPv4NumToString';

SELECT Y, uniqHLL12(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqHLL12(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqHLL12 remote()';

SELECT uniqHLL12(dummy) FROM remote('127.0.0.{2,3}', system.one);

-- uniqCombined

SELECT 'uniqCombined';

SELECT Y, uniqCombined(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(12)';

SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(17)';

SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(20)';

SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(round(float))';

SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(12)(round(float))';

SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(17)(round(float))';

SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(20)(round(float))';

SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(X)(round(toFloat32()))';

SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(12)(round(toFloat32()))';

SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(17)(round(toFloat32()))';

SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(20)(round(toFloat32()))';

SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(Z)(IPv4NumToString)';

SELECT Y, uniqCombined(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(12)(IPv4NumToString)';

SELECT Y, uniqCombined(12)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(12)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(17)(IPv4NumToString)';

SELECT Y, uniqCombined(17)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(17)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined(20)(IPv4NumToString)';

SELECT Y, uniqCombined(20)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y ORDER BY Y;
SELECT Y, uniqCombined(20)(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y ORDER BY Y;

SELECT 'uniqCombined remote()';

SELECT uniqCombined(dummy) FROM remote('127.0.0.{2,3}', system.one);
SELECT uniqCombined(12)(dummy) FROM remote('127.0.0.{2,3}', system.one);
SELECT uniqCombined(17)(dummy) FROM remote('127.0.0.{2,3}', system.one);
SELECT uniqCombined(20)(dummy) FROM remote('127.0.0.{2,3}', system.one);
