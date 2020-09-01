DROP TABLE IF EXISTS recompression_table;

CREATE TABLE recompression_table
(
    dt DateTime,
    key UInt64,
    value String

) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL dt + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), dt + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10));

SHOW CREATE TABLE recompression_table;

SYSTEM STOP TTL MERGES recompression_table;

INSERT INTO recompression_table SELECT now(), 1, toString(number) from numbers(1000);

INSERT INTO recompression_table SELECT now() - INTERVAL 2 MONTH, 2, toString(number) from numbers(1000, 1000);

INSERT INTO recompression_table SELECT now() - INTERVAL 2 YEAR, 3, toString(number) from numbers(2000, 1000);

SELECT COUNT() FROM recompression_table;

SELECT name, default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

ALTER TABLE recompression_table MODIFY TTL dt + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(12)) SETTINGS mutations_sync = 2;

SHOW CREATE TABLE recompression_table;

SELECT name, default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

SYSTEM START TTL MERGES recompression_table;

OPTIMIZE TABLE recompression_table FINAL;

SELECT name, default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

DROP TABLE IF EXISTS recompression_table;
