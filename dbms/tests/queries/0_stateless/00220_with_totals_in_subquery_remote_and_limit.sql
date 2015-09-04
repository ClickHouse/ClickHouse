SELECT x FROM (SELECT count() AS x FROM remote('127.0.0.1', system.one) WITH TOTALS) LIMIT 1;
