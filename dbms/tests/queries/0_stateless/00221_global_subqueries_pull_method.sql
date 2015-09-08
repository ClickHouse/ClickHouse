SET global_subqueries_method = 'push';
SELECT count() FROM remote('127.0.0.{1,2}', system.one) WHERE dummy GLOBAL IN (SELECT 0);
SELECT dummy, x FROM remote('127.0.0.{1,2}', system.one) GLOBAL ANY LEFT JOIN (SELECT 0 AS dummy, 1 AS x) USING dummy;
SET global_subqueries_method = 'pull';
SELECT count() FROM remote('127.0.0.{1,2}', system.one) WHERE dummy GLOBAL IN (SELECT 0);
SELECT dummy, x FROM remote('127.0.0.{1,2}', system.one) GLOBAL ANY LEFT JOIN (SELECT 0 AS dummy, 1 AS x) USING dummy;
