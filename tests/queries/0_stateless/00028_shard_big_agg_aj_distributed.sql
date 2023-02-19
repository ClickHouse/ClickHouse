-- Tags: distributed

DROP TABLE IF EXISTS big_array;
CREATE TABLE big_array (x Array(UInt8)) ENGINE=TinyLog;
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO big_array SELECT groupArray(number % 255) AS x FROM (SELECT * FROM system.numbers LIMIT 1000000);
SELECT sum(y) AS s FROM remote('127.0.0.{2,3}', currentDatabase(), big_array) ARRAY JOIN x AS y;
SELECT sum(s) FROM (SELECT y AS s FROM remote('127.0.0.{2,3}', currentDatabase(), big_array) ARRAY JOIN x AS y);
DROP TABLE big_array;
