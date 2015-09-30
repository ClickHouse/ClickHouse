SELECT k, a FROM (SELECT 42 AS k FROM remote('127.0.0.1', system.one)) GLOBAL ALL FULL OUTER JOIN (SELECT 42 AS k, 1 AS a, a) USING k;
SELECT 1 FROM remote('127.0.0.1', system.one) WHERE (1, 1) GLOBAL IN (SELECT 1 AS a, a);
