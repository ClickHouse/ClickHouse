-- A space-filling curve in a non-key-column position of the sorting key.
CREATE TABLE t_curve_nested (x UInt32, y UInt32)
ENGINE = MergeTree ORDER BY mortonEncode(hilbertEncode(x, y), x, y);
INSERT INTO t_curve_nested SELECT number, number FROM numbers(20000);
SELECT count() FROM t_curve_nested WHERE x >= 10 AND x <= 256 AND y >= 20 AND y <= 30;

-- Same, in the partition key.
CREATE TABLE t_curve_part (x UInt32, y UInt32)
ENGINE = MergeTree ORDER BY x PARTITION BY mortonEncode(x, y) % 4;
INSERT INTO t_curve_part SELECT number, number FROM numbers(20000);
SELECT count() FROM t_curve_part WHERE x >= 10 AND x <= 256 AND y >= 20 AND y <= 30;

-- Same, in a skip index expression.
CREATE TABLE t_curve_idx (x UInt32, y UInt32,
    INDEX i mortonEncode(hilbertEncode(x, y), x) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_curve_idx SELECT number, number FROM numbers(20000);
SELECT count() FROM t_curve_idx WHERE x >= 10 AND x <= 256 AND y >= 20 AND y <= 30;

-- Curve pruning still applies when the curve is a key column.
CREATE TABLE t_curve_plain (x UInt32, y UInt32)
ENGINE = MergeTree ORDER BY mortonEncode(x, y)
SETTINGS index_granularity = 8192;
INSERT INTO t_curve_plain SELECT number, number FROM numbers(20000);
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_curve_plain WHERE x >= 10 AND x <= 256 AND y >= 20 AND y <= 30)
    WHERE explain ILIKE '%has args in%';

-- Curve pruning still applies to a nested curve that is itself a key column.
CREATE TABLE t_curve_nested_key (x UInt32, y UInt32)
ENGINE = MergeTree ORDER BY (hilbertEncode(x, y), mortonEncode(hilbertEncode(x, y), x))
SETTINGS index_granularity = 8192;
INSERT INTO t_curve_nested_key SELECT number, number FROM numbers(20000);
SELECT count() > 0 FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_curve_nested_key WHERE x >= 10 AND x <= 256 AND y >= 20 AND y <= 30)
    WHERE explain ILIKE '%has args in%';

DROP TABLE t_curve_nested, t_curve_part, t_curve_idx, t_curve_plain, t_curve_nested_key;
