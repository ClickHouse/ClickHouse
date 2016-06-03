DROP TABLE IF EXISTS test.primary_key;
CREATE TABLE test.primary_key (d Date DEFAULT today(), x Int8) ENGINE = MergeTree(d, -x, 1);

INSERT INTO test.primary_key (x) VALUES (1), (2), (3);

SELECT x FROM test.primary_key ORDER BY x;

SELECT 'a', -x FROM test.primary_key WHERE -x < -3;
SELECT 'b', -x FROM test.primary_key WHERE -x < -2;
SELECT 'c', -x FROM test.primary_key WHERE -x < -1;
SELECT 'd', -x FROM test.primary_key WHERE -x < toInt8(0);

DROP TABLE test.primary_key;
