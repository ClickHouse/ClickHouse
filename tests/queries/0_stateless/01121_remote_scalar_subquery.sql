SELECT (SELECT 1) FROM remote('127.0.0.{1,2}', system.one);
SELECT (SELECT 1) FROM remote('127.0.0.{1,2}');
