-- Tags: shard

SELECT toFloat64(dummy + 2) AS n, j1, j2 FROM remote('127.0.0.{2,3}', system.one) jr1 GLOBAL ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10) jr2 USING n LIMIT 10;
