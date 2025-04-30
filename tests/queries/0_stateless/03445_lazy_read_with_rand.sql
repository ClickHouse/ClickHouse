DROP TABLE IF EXISTS test_03445_lazy;
CREATE TABLE test_03445_lazy (n UInt32) ENGINE = MergeTree ORDER BY n;
INSERT INTO test_03445_lazy SELECT * FROM generateRandom() LIMIT 50;

SELECT count() FROM (SELECT * FROM test_03445_lazy ORDER BY rand() LIMIT 1);
SELECT count() FROM (SELECT * FROM test_03445_lazy ORDER BY rand() LIMIT 5);
SELECT count() FROM (SELECT * FROM test_03445_lazy ORDER BY rand() LIMIT 10);
SELECT count() FROM (SELECT * FROM test_03445_lazy ORDER BY rand() LIMIT 11);
SELECT count() FROM (SELECT * FROM test_03445_lazy ORDER BY rand() LIMIT 50);
SELECT count() FROM (SELECT * FROM test_03445_lazy ORDER BY rand());

DROP TABLE test_03445_lazy;
