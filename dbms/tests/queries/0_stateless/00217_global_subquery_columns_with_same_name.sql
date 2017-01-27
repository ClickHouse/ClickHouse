SELECT k, a FROM (SELECT 42 AS k FROM remote('localhost', system.one)) GLOBAL ALL FULL OUTER JOIN (SELECT 42 AS k, 1 AS a, a) USING k;
SELECT 1 FROM remote('localhost', system.one) WHERE (1, 1) GLOBAL IN (SELECT 1 AS a, a);
