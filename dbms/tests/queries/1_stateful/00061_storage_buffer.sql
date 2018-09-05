DROP TABLE IF EXISTS test.hits_dst;
DROP TABLE IF EXISTS test.hits_buffer;

CREATE TABLE test.hits_dst AS test.hits;
CREATE TABLE test.hits_buffer AS test.hits_dst ENGINE = Buffer(test, hits_dst, 8, 1, 10, 10000, 100000, 10000000, 100000000);

INSERT INTO test.hits_buffer SELECT * FROM test.hits WHERE CounterID = 101500;
SELECT count() FROM test.hits_buffer;
SELECT count() FROM test.hits_dst;

OPTIMIZE TABLE test.hits_buffer;
SELECT count() FROM test.hits_buffer;
SELECT count() FROM test.hits_dst;

DROP TABLE test.hits_dst;
DROP TABLE test.hits_buffer;
