--Tags: no-fasttest, no-cpu-aarch64, no-cpu-s390x
-- no-fasttest because DEFLATE_QPL isn't available in fasttest
-- no-cpu-aarch64 and no-cpu-s390x because DEFLATE_QPL is x86-only

-- A bunch of random DDLs to test the DEFLATE_QPL codec.

SET enable_deflate_qpl_codec = 1;

-- Suppress test failures because stderr contains warning "Initialization of hardware-assisted DeflateQpl failed, falling
-- back to software DeflateQpl coded."
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS compression_codec;

CREATE TABLE compression_codec(
    id UInt64 CODEC(DEFLATE_QPL),
    data String CODEC(DEFLATE_QPL),
    ddd Date CODEC(DEFLATE_QPL),
    ddd32 Date32 CODEC(DEFLATE_QPL),
    somenum Float64 CODEC(DEFLATE_QPL),
    somestr FixedString(3) CODEC(DEFLATE_QPL),
    othernum Int64 CODEC(DEFLATE_QPL),
    somearray Array(UInt8) CODEC(DEFLATE_QPL),
    somemap Map(String, UInt32) CODEC(DEFLATE_QPL),
    sometuple Tuple(UInt16, UInt64) CODEC(DEFLATE_QPL),
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
