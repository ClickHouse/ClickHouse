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
ORDER BY tuple();

INSERT INTO test_2313(id,data,ddd,somenum,somestr) VALUES (101,'Hello,ClickHouse!',now(),1.0,'a');

OPTIMIZE TABLE test_2313;
SELECT column,compression_codec FROM system.parts_columns WHERE table='test_2313';
DROP TABLE test_2313;
