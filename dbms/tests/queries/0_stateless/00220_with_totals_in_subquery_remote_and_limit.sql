SELECT x FROM (SELECT count() AS x FROM remote('localhost', system.one) WITH TOTALS) LIMIT 1;
