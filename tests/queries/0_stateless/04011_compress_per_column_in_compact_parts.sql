DROP TABLE IF EXISTS t_compact_flush_true;
DROP TABLE IF EXISTS t_compact_flush_false;

-- Table with per-column compressed blocks (default behavior)
CREATE TABLE t_compact_flush_true (id UInt64, v1 UInt64, v2 UInt64, v3 UInt64, v4 UInt64, v5 UInt64, v6 UInt64, v7 UInt64, v8 UInt64, v9 UInt64, v10 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 100000000, compress_per_column_in_compact_parts = true;

-- Table with shared compressed blocks per granule
CREATE TABLE t_compact_flush_false (id UInt64, v1 UInt64, v2 UInt64, v3 UInt64, v4 UInt64, v5 UInt64, v6 UInt64, v7 UInt64, v8 UInt64, v9 UInt64, v10 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 100000000, compress_per_column_in_compact_parts = false;

INSERT INTO t_compact_flush_true SELECT number, number+1, number+2, number+3, number+4, number+5, number+6, number+7, number+8, number+9, number+10 FROM numbers(5000);
INSERT INTO t_compact_flush_false SELECT number, number+1, number+2, number+3, number+4, number+5, number+6, number+7, number+8, number+9, number+10 FROM numbers(5000);

-- Both should produce Compact parts
SELECT 'true_part_type', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_flush_true' AND active;
SELECT 'false_part_type', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_flush_false' AND active;

-- Data should be identical regardless of setting (all columns)
SELECT 'true_hash', groupBitXor(sipHash64(*)) FROM t_compact_flush_true;
SELECT 'false_hash', groupBitXor(sipHash64(*)) FROM t_compact_flush_false;

-- Partial column reads: reading a subset of columns should work with both settings
SELECT 'true_partial', sum(id), sum(v5) FROM t_compact_flush_true;
SELECT 'false_partial', sum(id), sum(v5) FROM t_compact_flush_false;

-- Single column read
SELECT 'true_single', sum(v10) FROM t_compact_flush_true;
SELECT 'false_single', sum(v10) FROM t_compact_flush_false;

-- Dynamic setting modification: change from true to false and verify new parts use the new layout
ALTER TABLE t_compact_flush_true MODIFY SETTING compress_per_column_in_compact_parts = false;
INSERT INTO t_compact_flush_true SELECT number+5000, number+5001, number+5002, number+5003, number+5004, number+5005, number+5006, number+5007, number+5008, number+5009, number+5010 FROM numbers(5000);

-- Should have 2 parts now, both readable despite different compression layouts
SELECT 'after_alter_count', count() FROM t_compact_flush_true;
SELECT 'after_alter_partial', sum(id), sum(v3) FROM t_compact_flush_true;

-- Change from false to true and verify
ALTER TABLE t_compact_flush_false MODIFY SETTING compress_per_column_in_compact_parts = true;
INSERT INTO t_compact_flush_false SELECT number+5000, number+5001, number+5002, number+5003, number+5004, number+5005, number+5006, number+5007, number+5008, number+5009, number+5010 FROM numbers(5000);

SELECT 'after_alter_count2', count() FROM t_compact_flush_false;
SELECT 'after_alter_partial2', sum(id), sum(v3) FROM t_compact_flush_false;

-- Merge parts with mixed layouts and verify data integrity
OPTIMIZE TABLE t_compact_flush_true FINAL;
OPTIMIZE TABLE t_compact_flush_false FINAL;

SELECT 'merged_hash_true', groupBitXor(sipHash64(*)) FROM t_compact_flush_true;
SELECT 'merged_hash_false', groupBitXor(sipHash64(*)) FROM t_compact_flush_false;

-- Both tables should have identical data and hash after merge
SELECT 'merged_match', (SELECT groupBitXor(sipHash64(*)) FROM t_compact_flush_true) = (SELECT groupBitXor(sipHash64(*)) FROM t_compact_flush_false);

DROP TABLE t_compact_flush_true;
DROP TABLE t_compact_flush_false;
