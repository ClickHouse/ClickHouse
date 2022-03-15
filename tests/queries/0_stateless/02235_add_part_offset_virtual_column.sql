DROP TABLE IF EXISTS t_1;
DROP TABLE IF EXISTS t_random_1;

CREATE TABLE t_1
(
    `order_0` UInt64,
    `ordinary_1` UInt32,
    `p_time` Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(p_time)
ORDER BY order_0;

CREATE TABLE t_random_1
(
    `ordinary_1` UInt32
)
ENGINE = GenerateRandom(1, 5, 3);

INSERT INTO t_1 select rowNumberInAllBlocks(),*,NOW() from t_random_1 limit 1000;

SELECT max(_part_offset) FROM t_1;
SELECT count(*) FROM t_1 WHERE _part_offset != order_0;
SELECT count(*) FROM t_1 WHERE order_0 IN (SELECT toUInt64(rand64()%1000) FROM system.numbers limit 100) AND _part_offset != order_0;
SELECT count(*) FROM t_1 PREWHERE ordinary_1 > 5000 WHERE _part_offset != order_0;