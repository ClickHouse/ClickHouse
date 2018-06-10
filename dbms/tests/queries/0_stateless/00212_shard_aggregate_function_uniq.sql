/* uniqHLL12 */

SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqHLL12(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT Y, uniqHLL12(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqHLL12(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqHLL12(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT uniqHLL12(dummy) FROM remote('127.0.0.{2,3}', system.one);

/* uniqCombined */

SELECT Y, uniqCombined(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(1/(1 + (3*X*X - 7*X + 11) % 37), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqCombined(X) FROM (SELECT number AS X, round(toFloat32(1/(1 + (3*X*X - 7*X + 11) % 37)), 3) AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT Y, uniqCombined(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 15) GROUP BY Y;
SELECT Y, uniqCombined(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 3000) GROUP BY Y;
SELECT Y, uniqCombined(Z) FROM (SELECT number AS X, IPv4NumToString(toUInt32(X)) AS Z, (3*X*X - 7*X + 11) % 37 AS Y FROM system.numbers LIMIT 1000000) GROUP BY Y;

SELECT uniqCombined(dummy) FROM remote('127.0.0.{2,3}', system.one);
