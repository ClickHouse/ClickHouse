SET enable_analyzer=1;
SELECT 1 GLOBAL IN (SELECT 1) AS s, s FROM remote('127.0.0.{2,3}', system.one) GROUP BY 1;
SELECT 1 GLOBAL IN (SELECT 1) AS s FROM remote('127.0.0.{2,3}', system.one) GROUP BY 1;

SELECT 1 GLOBAL IN (SELECT 1) AS s, s FROM remote('127.0.0.{1,3}', system.one) GROUP BY 1;
SELECT 1 GLOBAL IN (SELECT 1) AS s FROM remote('127.0.0.{1,3}', system.one) GROUP BY 1;
