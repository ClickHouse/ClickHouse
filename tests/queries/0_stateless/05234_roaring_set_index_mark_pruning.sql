-- Test MergeTreeSetIndex Roaring Bitmap fast path for single integer key mark pruning.
DROP TABLE IF EXISTS t_roaring_index;
DROP TABLE IF EXISTS t_roaring_index_u32;

CREATE TABLE t_roaring_index (id UInt64, value Float64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_roaring_index SELECT number, number * 1.5 FROM numbers(100);

SELECT '--- Basic IN subset ---';
SELECT id FROM t_roaring_index WHERE id IN (SELECT number FROM numbers(100) WHERE number % 20 == 0) ORDER BY id;

SELECT '--- Point lookup via IN ---';
SELECT id, value FROM t_roaring_index WHERE id IN (SELECT 42::UInt64);

SELECT '--- Narrow range in set ---';
SELECT count() FROM t_roaring_index WHERE id IN (SELECT number FROM numbers(100) WHERE number BETWEEN 10 AND 12);

SELECT '--- Single element NOT IN ---';
SELECT id FROM t_roaring_index WHERE id NOT IN (SELECT number FROM numbers(100) WHERE number != 50) ORDER BY id;

SELECT '--- UInt32 key column ---';
CREATE TABLE t_roaring_index_u32 (id UInt32, value Float64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_roaring_index_u32 SELECT number, number * 2.0 FROM numbers(50);

SELECT id FROM t_roaring_index_u32 WHERE id IN (SELECT toUInt32(number * 10) FROM numbers(5)) ORDER BY id;

DROP TABLE t_roaring_index;
DROP TABLE t_roaring_index_u32;
