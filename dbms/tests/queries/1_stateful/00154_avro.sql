DROP TABLE IF EXISTS test.avro;

CREATE TABLE test.avro AS test.hits ENGINE = File(Avro);
INSERT INTO test.avro SELECT * FROM test.hits WHERE intHash64(WatchID) % 100 = 0;

SELECT sum(cityHash64(*)) FROM test.hits WHERE intHash64(WatchID) % 100 = 0;
SELECT sum(cityHash64(*)) FROM test.avro;

DROP TABLE test.avro;
