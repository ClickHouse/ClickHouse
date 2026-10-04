-- Checks that a `bloom_filter` index skips no matching row when a `String` or `FixedString` constant
-- compares zero-padded, and that it stays usable and prunes where padding gives the one value that can match.

DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_fs3;
DROP TABLE IF EXISTS t_lcfs;
CREATE TABLE t_str (id UInt64, v String, INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_fs3 (id UInt64, v FixedString(3), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_lcfs (id UInt64, v LowCardinality(FixedString(3)), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_str VALUES (0, 'V0'), (1, 'V0\0'), (2, 'V0\0\0'), (3, 'V0X'), (4, 'X');
INSERT INTO t_fs3 VALUES (0, 'V0'), (1, 'V0X'), (2, 'X');
INSERT INTO t_lcfs VALUES (0, 'V0'), (1, 'V0X'), (2, 'X');

SELECT 'String index, wrapped FixedString constant', count() FROM t_str WHERE v = CAST(toFixedString('V0', 3) AS LowCardinality(Nullable(FixedString(3))));
SELECT 'String index, Variant constant', count() FROM t_str WHERE v = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT 'String index, Dynamic constant', count() FROM t_str WHERE v = CAST(toFixedString('V0', 3) AS Dynamic);
SELECT 'LowCardinality(FixedString) index, wider String constant', count() FROM t_lcfs WHERE v = 'V0\0\0';
SELECT 'FixedString index, wider String constant', count() FROM t_fs3 WHERE v = 'V0\0\0';
SELECT 'FixedString index, wider FixedString constant', count() FROM t_fs3 WHERE v = toFixedString('V0', 4);

SELECT count() FROM t_str WHERE v = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'String index used, String constant', count() FROM t_str WHERE v = 'V0' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'FixedString index used, narrower FixedString constant', count() FROM t_fs3 WHERE v = toFixedString('V0', 2) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'FixedString index used, equal width FixedString constant', count() FROM t_fs3 WHERE v = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'FixedString index used, plain String constant', count() FROM t_fs3 WHERE v = 'V0' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'FixedString index prunes, equal width FixedString constant', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fs3 WHERE v = toFixedString('V0', 3)) WHERE explain ILIKE '%Granules: 1/3%';
