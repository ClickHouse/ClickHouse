SELECT * FROM (SELECT x FROM (SELECT toString(number) AS x FROM system.numbers LIMIT 2000000) ORDER BY x LIMIT 10000) LIMIT 10;
SET max_bytes_before_remerge_sort = 1000000;
SELECT * FROM (SELECT x FROM (SELECT toString(number) AS x FROM system.numbers LIMIT 2000000) ORDER BY x LIMIT 10000) LIMIT 10;
