
DROP TABLE IF EXISTS test_2313;
CREATE TABLE test_2313
(
    `id` UInt64 CODEC(LZ4),
    `data` String CODEC(ZSTD),
    `ddd` Date CODEC(NONE),
    `somenum` Float64 CODEC(ZSTD(2)),
    `somestr` FixedString(3) CODEC(LZ4HC(7))
)
ENGINE = MergeTree
ORDER BY tuple()
-- Force this table to use the wide format
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_2313(id,data,ddd,somenum,somestr) VALUES (101,'Hello,ClickHouse!',now(),1.0,'a');

OPTIMIZE TABLE test_2313;
SELECT column,compression_codec FROM system.parts_columns WHERE database = currentDatabase() AND table='test_2313';
DROP TABLE test_2313;
