SELECT 1 GLOBAL IN (SELECT 1) AS s, s FROM remote('127.0.0.{1,2}', system.one);
