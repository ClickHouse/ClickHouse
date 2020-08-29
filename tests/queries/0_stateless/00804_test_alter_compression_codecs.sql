SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS alter_compression_codec;

CREATE TABLE alter_compression_codec (
    somedate Date CODEC(LZ4),
    id UInt64 CODEC(NONE)
) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id;

INSERT INTO alter_compression_codec VALUES('2018-01-01', 1);
INSERT INTO alter_compression_codec VALUES('2018-01-01', 2);
SELECT * FROM alter_compression_codec ORDER BY id;

ALTER TABLE alter_compression_codec ADD COLUMN alter_column String DEFAULT 'default_value' CODEC(ZSTD);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';

INSERT INTO alter_compression_codec VALUES('2018-01-01', 3, '3');
INSERT INTO alter_compression_codec VALUES('2018-01-01', 4, '4');
SELECT * FROM alter_compression_codec ORDER BY id;

ALTER TABLE alter_compression_codec MODIFY COLUMN alter_column CODEC(NONE);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';

INSERT INTO alter_compression_codec VALUES('2018-01-01', 5, '5');
INSERT INTO alter_compression_codec VALUES('2018-01-01', 6, '6');
SELECT * FROM alter_compression_codec ORDER BY id;

OPTIMIZE TABLE alter_compression_codec FINAL;
SELECT * FROM alter_compression_codec ORDER BY id;

SET allow_suspicious_codecs = 1;
ALTER TABLE alter_compression_codec MODIFY COLUMN alter_column CODEC(ZSTD, LZ4HC, LZ4, LZ4, NONE);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';

INSERT INTO alter_compression_codec VALUES('2018-01-01', 7, '7');
INSERT INTO alter_compression_codec VALUES('2018-01-01', 8, '8');
OPTIMIZE TABLE alter_compression_codec FINAL;
SELECT * FROM alter_compression_codec ORDER BY id;

ALTER TABLE alter_compression_codec MODIFY COLUMN alter_column FixedString(100);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';


DROP TABLE IF EXISTS alter_compression_codec;

DROP TABLE IF EXISTS alter_bad_codec;

CREATE TABLE alter_bad_codec (
    somedate Date CODEC(LZ4),
    id UInt64 CODEC(NONE)
) ENGINE = MergeTree() ORDER BY tuple();

ALTER TABLE alter_bad_codec ADD COLUMN alter_column DateTime DEFAULT '2019-01-01 00:00:00' CODEC(gbdgkjsdh); -- { serverError 432 }

ALTER TABLE alter_bad_codec ADD COLUMN alter_column DateTime DEFAULT '2019-01-01 00:00:00' CODEC(ZSTD(100)); -- { serverError 433 }

DROP TABLE IF EXISTS alter_bad_codec;

DROP TABLE IF EXISTS large_alter_table_00804;
DROP TABLE IF EXISTS store_of_hash_00804;

CREATE TABLE large_alter_table_00804 (
    somedate Date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
    id UInt64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
    data String CODEC(ZSTD(2), LZ4HC, NONE, LZ4, LZ4)
) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2;

INSERT INTO large_alter_table_00804 SELECT toDate('2019-01-01'), number, toString(number + rand()) FROM system.numbers LIMIT 300000;

CREATE TABLE store_of_hash_00804 (hash UInt64) ENGINE = Memory();

INSERT INTO store_of_hash_00804 SELECT sum(cityHash64(*)) FROM large_alter_table_00804;

ALTER TABLE large_alter_table_00804 MODIFY COLUMN data CODEC(NONE, LZ4, LZ4HC, ZSTD);

OPTIMIZE TABLE large_alter_table_00804;

SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'large_alter_table_00804' AND name = 'data';

DETACH TABLE large_alter_table_00804;
ATTACH TABLE large_alter_table_00804;

INSERT INTO store_of_hash_00804 SELECT sum(cityHash64(*)) FROM large_alter_table_00804;

SELECT COUNT(hash) FROM store_of_hash_00804;
SELECT COUNT(DISTINCT hash) FROM store_of_hash_00804;

DROP TABLE IF EXISTS large_alter_table_00804;
DROP TABLE IF EXISTS store_of_hash_00804;
