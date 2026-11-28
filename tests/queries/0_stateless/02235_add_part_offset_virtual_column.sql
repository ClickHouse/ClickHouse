DROP TABLE IF EXISTS t_1;
DROP TABLE IF EXISTS t_random_1;

CREATE TABLE t_1
(
    `order_0` UInt64,
    `ordinary_1` UInt32,
    `p_time` Date,
    `computed` ALIAS 'computed_' || cast(`p_time` AS String),
    `granule` MATERIALIZED cast(`order_0` / 0x2000 AS UInt64) % 3,
    INDEX `index_granule` `granule` TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(p_time)
ORDER BY order_0
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE t_random_1
(
    `ordinary_1` UInt32
)
ENGINE = GenerateRandom(1, 5, 3);

SET optimize_trivial_insert_select = 1;
INSERT INTO t_1 select rowNumberInAllBlocks(), *, '1984-01-01' from t_random_1 limit 1000000;

OPTIMIZE TABLE t_1 FINAL;

ALTER TABLE t_1 ADD COLUMN foo String DEFAULT 'foo';

SELECT COUNT(DISTINCT(_part)) FROM t_1;

SELECT min(_part_offset), max(_part_offset) FROM t_1;
SELECT count(*) FROM t_1 WHERE _part_offset != order_0;
SELECT count(*) FROM t_1 WHERE order_0 IN (SELECT toUInt64(rand64()%1000) FROM system.numbers limit 100) AND _part_offset != order_0;
SELECT count(*) FROM t_1 PREWHERE ordinary_1 > 5000 WHERE _part_offset != order_0;
SELECT order_0, _part_offset, _part FROM t_1 ORDER BY order_0 LIMIT 3;
SELECT order_0, _part_offset, _part FROM t_1 ORDER BY order_0 DESC LIMIT 3;
SELECT order_0, _part_offset, _part FROM t_1 WHERE order_0 <= 1 OR (order_0 BETWEEN 10000 AND 10002) OR order_0 >= 999998 ORDER BY order_0;
SELECT order_0, _part_offset, _part FROM t_1 WHERE order_0 <= 1 OR (order_0 BETWEEN 10000 AND 10002) OR order_0 >= 999998 ORDER BY order_0 DESC;
SELECT order_0, _part_offset, computed FROM t_1 ORDER BY order_0, _part_offset, computed LIMIT 3;
SELECT order_0, _part_offset, computed FROM t_1 ORDER BY order_0 DESC, _part_offset DESC, computed DESC LIMIT 3;
SELECT order_0, _part_offset, _part FROM t_1 WHERE order_0 <= 1 OR order_0 >= 999998 ORDER BY order_0 LIMIT 3;
SELECT _part_offset FROM t_1 ORDER BY order_0 LIMIT 3;
SELECT _part_offset, foo FROM t_1 ORDER BY order_0 LIMIT 3;

SELECT 'SOME GRANULES FILTERED OUT';
SELECT count(*), sum(_part_offset), sum(order_0) from t_1 where granule == 0;
SELECT count(*), sum(_part_offset), sum(order_0) from t_1 where granule == 0 AND _part_offset < 100000;
SELECT count(*), sum(_part_offset), sum(order_0) from t_1 where granule == 0 AND _part_offset >= 100000;
SELECT _part_offset FROM t_1 where granule == 0 AND _part_offset >= 100000 ORDER BY order_0 LIMIT 3;
SELECT _part_offset, foo FROM t_1 where granule == 0 AND _part_offset >= 100000 ORDER BY order_0 LIMIT 3;

SELECT 'PREWHERE';
SELECT count(*), sum(_part_offset), sum(order_0) from t_1 prewhere granule == 0 where _part_offset >= 100000;
SELECT count(*), sum(_part_offset), sum(order_0) from t_1 prewhere _part != '' where granule == 0;
SELECT count(*), sum(_part_offset), sum(order_0) from t_1 prewhere _part_offset > 100000 where granule == 0;
SELECT _part_offset FROM t_1 PREWHERE order_0 % 10000 == 42 ORDER BY order_0 LIMIT 3;
SELECT _part_offset, foo FROM t_1 PREWHERE order_0 % 10000 == 42 ORDER BY order_0 LIMIT 3;
