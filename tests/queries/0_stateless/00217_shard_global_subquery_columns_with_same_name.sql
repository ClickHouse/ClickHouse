SET joined_subquery_requires_alias = 0;

SELECT k, a FROM (SELECT 42 AS k FROM remote('127.0.0.2', system.one)) GLOBAL ALL FULL OUTER JOIN (SELECT 42 AS k, 1 AS a, a) USING k;
SELECT 1 FROM remote('127.0.0.2', system.one) WHERE (1, 1) GLOBAL IN (SELECT 1 AS a, a);
