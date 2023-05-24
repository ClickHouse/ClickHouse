SET send_logs_level = 'fatal';
SET enable_qpl_deflate = 1;

DROP TABLE IF EXISTS compression_codec;

CREATE TABLE compression_codec(
    id UInt64 CODEC(DEFLATE_QPL),
    data String CODEC(DEFLATE_QPL),
    ddd Date CODEC(DEFLATE_QPL),
    somenum Float64 CODEC(DEFLATE_QPL),
    somestr FixedString(3) CODEC(DEFLATE_QPL),
    othernum Int64 CODEC(DEFLATE_QPL),
    qplstr String CODEC(DEFLATE_QPL),
    qplnum UInt32 CODEC(DEFLATE_QPL),
) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO compression_codec VALUES(1, 'hello', toDate('2018-12-14'), 1.1, 'aaa', 5, 'qpl11', 11);
INSERT INTO compression_codec VALUES(2, 'world', toDate('2018-12-15'), 2.2, 'bbb', 6,'qpl22', 22);
INSERT INTO compression_codec VALUES(3, '!', toDate('2018-12-16'), 3.3, 'ccc', 7, 'qpl33', 33);

SELECT * FROM compression_codec ORDER BY id;

OPTIMIZE TABLE compression_codec FINAL;

INSERT INTO compression_codec VALUES(2, '', toDate('2018-12-13'), 4.4, 'ddd', 8, 'qpl44', 44);

DETACH TABLE compression_codec;
ATTACH TABLE compression_codec;

SELECT count(*) FROM compression_codec WHERE id = 2 GROUP BY id;

DROP TABLE IF EXISTS compression_codec;
