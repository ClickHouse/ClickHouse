-- Tags: shard

SELECT x FROM (SELECT count() AS x FROM remote('127.0.0.2', system.one) WITH TOTALS) LIMIT 1;
