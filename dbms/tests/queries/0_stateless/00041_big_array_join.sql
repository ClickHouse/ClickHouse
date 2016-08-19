CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.big_array;
CREATE TABLE test.big_array (x Array(UInt8)) ENGINE=TinyLog;
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO test.big_array SELECT groupArray(number % 255) AS x FROM (SELECT * FROM system.numbers LIMIT 1000000);

SELECT count() FROM test.big_array ARRAY JOIN x;
SELECT count() FROM test.big_array ARRAY JOIN x AS y;
SELECT countIf(has(x, 10)), sum(y) FROM test.big_array ARRAY JOIN x AS y;
SELECT countIf(has(x, 10)) FROM test.big_array ARRAY JOIN x AS y;
SELECT countIf(has(x, 10)), sum(y) FROM test.big_array ARRAY JOIN x AS y WHERE 1;
SELECT countIf(has(x, 10)) FROM test.big_array ARRAY JOIN x AS y WHERE 1;
SELECT countIf(has(x, 10)), sum(y) FROM test.big_array ARRAY JOIN x AS y WHERE has(x,15);

DROP TABLE test.big_array;
