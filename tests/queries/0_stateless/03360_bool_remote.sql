SELECT true AS x FROM remote('127.0.0.{1,2}', system.one) LIMIT 1;
SELECT materialize(true) AS x FROM remote('127.0.0.{1,2}', system.one) LIMIT 1;
SELECT true AS x FROM remote('127.0.0.{1,2}', system.one) GROUP BY x;
