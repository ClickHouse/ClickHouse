SET send_logs_level = 'none';
DROP TABLE IF EXISTS test.compression_codec;

CREATE TABLE test.compression_codec(id UInt64 CODEC(LZ4), data String CODEC(ZSTD), ddd Date CODEC(NONE), somenum Float64 CODEC(ZSTD(2))) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO test.compression_codec VALUES(1, 'hello', toDate('2018-12-14'), 1.1);
INSERT INTO test.compression_codec VALUES(2, 'world', toDate('2018-12-15'), 2.2);
INSERT INTO test.compression_codec VALUES(3, '!', toDate('2018-12-16'), 3.3);

SELECT * FROM test.compression_codec ORDER BY id;

OPTIMIZE TABLE test.compression_codec FINAL;

INSERT INTO test.compression_codec VALUES(2, '', toDate('2018-12-13'), 4.4);

SELECT count(*) FROM test.compression_codec WHERE id = 2 GROUP BY id;

DROP TABLE IF EXISTS test.compression_codec;

DROP TABLE IF EXISTS test.bad_codec;
DROP TABLE IF EXISTS test.params_when_no_params;
DROP TABLE IF EXISTS test.too_many_params;

CREATE TABLE test.bad_codec(id UInt64 CODEC(adssadads)) ENGINE = MergeTree() order by tuple(); -- { serverError 429 }
CREATE TABLE test.too_many_params(id UInt64 CODEC(ZSTD(2,3,4,5))) ENGINE = MergeTree() order by tuple(); -- { serverError 428 }
CREATE TABLE test.params_when_no_params(id UInt64 CODEC(LZ4(1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 378 }

DROP TABLE IF EXISTS test.bad_codec;
DROP TABLE IF EXISTS test.params_when_no_params;
DROP TABLE IF EXISTS test.too_many_params;
