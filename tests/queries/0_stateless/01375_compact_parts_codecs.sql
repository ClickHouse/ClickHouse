-- Tags: no-parallel

DROP TABLE IF EXISTS codecs;

CREATE TABLE codecs (id UInt32, val UInt32, s String) 
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 10000;
INSERT INTO codecs SELECT number, number, toString(number) FROM numbers(1000);
SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) 
    FROM system.parts 
    WHERE table = 'codecs' AND database = currentDatabase();

SELECT sum(id), sum(val), max(s) FROM codecs;

DETACH TABLE codecs;
ATTACH table codecs;

SELECT sum(id), sum(val), max(s) FROM codecs;

DROP TABLE codecs;

CREATE TABLE codecs (id UInt32 CODEC(NONE), val UInt32 CODEC(NONE), s String CODEC(NONE)) 
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 10000;
INSERT INTO codecs SELECT number, number, toString(number) FROM numbers(1000);
SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) 
    FROM system.parts 
    WHERE table = 'codecs' AND database = currentDatabase();

SELECT sum(id), sum(val), max(s) FROM codecs;

DETACH TABLE codecs;
ATTACH table codecs;

SELECT sum(id), sum(val), max(s) FROM codecs;

DROP TABLE codecs;

CREATE TABLE codecs (id UInt32, val UInt32 CODEC(Delta, ZSTD), s String CODEC(ZSTD)) 
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 10000;
INSERT INTO codecs SELECT number, number, toString(number) FROM numbers(1000);
SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) 
    FROM system.parts 
    WHERE table = 'codecs' AND database = currentDatabase();

SELECT sum(id), sum(val), max(s) FROM codecs;

DETACH TABLE codecs;
ATTACH table codecs;

SELECT sum(id), sum(val), max(s) FROM codecs;

DROP TABLE codecs;
