DROP TABLE IF EXISTS test.compression_codec;

CREATE TABLE test.compression_codec(id UInt64 CODEC(LZ4), data String CODEC(ZSTD)) ENGINE = MergeTree() order by tuple();

INSERT INTO test.compression_codec VALUES(1, 'hello');
INSERT INTO test.compression_codec VALUES(2, 'world');
INSERT INTO test.compression_codec VALUES(3, '!');

SELECT * FROM test.compression_codec;

DROP TABLE IF EXISTS test.compression_codec;
