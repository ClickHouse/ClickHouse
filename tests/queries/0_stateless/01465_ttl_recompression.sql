DROP TABLE IF EXISTS recompression_table;

CREATE TABLE recompression_table
(
    dt DateTime,
    key UInt64,
    value String

) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL dt + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), dt + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SHOW CREATE TABLE recompression_table;

SYSTEM STOP TTL MERGES recompression_table;

INSERT INTO recompression_table SELECT now(), 1, toString(number) from numbers(1000);

INSERT INTO recompression_table SELECT now() - INTERVAL 2 MONTH, 2, toString(number) from numbers(1000, 1000);

INSERT INTO recompression_table SELECT now() - INTERVAL 2 YEAR, 3, toString(number) from numbers(2000, 1000);

SELECT COUNT() FROM recompression_table;

SELECT substring(name, 1, length(name) - 2), default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

OPTIMIZE TABLE recompression_table FINAL;

-- merge level and mutation in part name is not important
SELECT substring(name, 1, length(name) - 2), default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

ALTER TABLE recompression_table MODIFY TTL dt + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(12)) SETTINGS mutations_sync = 2;

SHOW CREATE TABLE recompression_table;

SELECT substring(name, 1, length(name) - 4), default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

SYSTEM START TTL MERGES recompression_table;
-- Additional merge can happen here
OPTIMIZE TABLE recompression_table FINAL;

-- merge level and mutation in part name is not important
SELECT substring(name, 1, length(name) - 4), default_compression_codec FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

SELECT substring(name, 1, length(name) - 4), recompression_ttl_info.expression FROM system.parts WHERE table = 'recompression_table' and active = 1 and database = currentDatabase() ORDER BY name;

DROP TABLE IF EXISTS recompression_table;

CREATE TABLE recompression_table_compact
(
  dt DateTime,
  key UInt64,
  value String

) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL dt + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), dt + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 10000;

SYSTEM STOP TTL MERGES recompression_table_compact;

INSERT INTO recompression_table_compact SELECT now(), 1, toString(number) from numbers(1000);

INSERT INTO recompression_table_compact SELECT now() - INTERVAL 2 MONTH, 2, toString(number) from numbers(1000, 1000);

INSERT INTO recompression_table_compact SELECT now() - INTERVAL 2 YEAR, 3, toString(number) from numbers(2000, 1000);

SELECT substring(name, 1, length(name) - 2), default_compression_codec FROM system.parts WHERE table = 'recompression_table_compact' and active = 1 and database = currentDatabase() ORDER BY name;

ALTER TABLE recompression_table_compact MODIFY TTL dt + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(12)) SETTINGS mutations_sync = 2; -- mutation affect all columns, so codec changes

-- merge level and mutation in part name is not important
SELECT substring(name, 1, length(name) - 4), default_compression_codec FROM system.parts WHERE table = 'recompression_table_compact' and active = 1 and database = currentDatabase() ORDER BY name;

DROP TABLE recompression_table_compact;
