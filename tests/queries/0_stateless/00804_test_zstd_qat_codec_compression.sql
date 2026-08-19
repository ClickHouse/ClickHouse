--Tags: no-fasttest, no-cpu-aarch64, no-cpu-s390x
-- no-fasttest because ZSTD_QAT isn't available in fasttest
-- no-cpu-aarch64 and no-cpu-s390x because ZSTD_QAT is x86-only

SET enable_zstd_qat_codec = 1;

-- Suppress test failures because stderr contains warning "Initialization of hardware-assisted ZSTD_QAT codec failed, falling back to software ZSTD coded."
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS compression_codec;

-- negative test
CREATE TABLE compression_codec(id UInt64 CODEC(ZSTD_QAT(0))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE compression_codec(id UInt64 CODEC(ZSTD_QAT(13))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

CREATE TABLE compression_codec(
    id UInt64 CODEC(ZSTD_QAT),
    data String CODEC(ZSTD_QAT),
    ddd Date CODEC(ZSTD_QAT),
    ddd32 Date32 CODEC(ZSTD_QAT),
    somenum Float64 CODEC(ZSTD_QAT),
    somestr FixedString(3) CODEC(ZSTD_QAT),
    othernum Int64 CODEC(ZSTD_QAT),
    somearray Array(UInt8) CODEC(ZSTD_QAT),
    somemap Map(String, UInt32) CODEC(ZSTD_QAT),
    sometuple Tuple(UInt16, UInt64) CODEC(ZSTD_QAT),
) ENGINE = MergeTree() ORDER BY tuple();

SHOW CREATE TABLE compression_codec;

INSERT INTO compression_codec VALUES(1, 'hello', toDate('2018-12-14'), toDate32('2018-12-14'), 1.1, 'aaa', 5, [1,2,3], map('k1',1,'k2',2), tuple(1,2));
INSERT INTO compression_codec VALUES(2, 'world', toDate('2018-12-15'), toDate32('2018-12-15'), 2.2, 'bbb', 6, [4,5,6], map('k3',3,'k4',4), tuple(3,4));
INSERT INTO compression_codec VALUES(3, '!', toDate('2018-12-16'), toDate32('2018-12-16'), 3.3, 'ccc', 7, [7,8,9], map('k5',5,'k6',6), tuple(5,6));

SELECT * FROM compression_codec ORDER BY id;

OPTIMIZE TABLE compression_codec FINAL;

INSERT INTO compression_codec VALUES(2, '', toDate('2018-12-13'), toDate32('2018-12-13'), 4.4, 'ddd', 8, [10,11,12], map('k7',7,'k8',8), tuple(7,8));

DETACH TABLE compression_codec;
ATTACH TABLE compression_codec;

SELECT count(*) FROM compression_codec WHERE id = 2 GROUP BY id;

INSERT INTO compression_codec SELECT 3, '!', toDate('2018-12-16'), toDate32('2018-12-16'), 3.3, 'ccc', 7, [7,8,9], map('k5',5,'k6',6), tuple(5,6) FROM system.numbers LIMIT 10000;

SELECT count(*) FROM compression_codec WHERE id = 3 GROUP BY id;

DROP TABLE IF EXISTS compression_codec;
