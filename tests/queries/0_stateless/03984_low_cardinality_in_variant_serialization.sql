-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/97700
-- LowCardinality inside a Variant type could cause a bad cast exception
-- when the column was unwrapped but the data type still expected ColumnLowCardinality.
SET use_variant_as_common_type = 1;

-- Text serialization path (serializeImpl)
SELECT concat([toLowCardinality(17), 42, 'x'], 'a');
SELECT concat([[toLowCardinality(17), 42]], 'b');

-- Binary serialization path (serializeBinaryBulkWithMultipleStreams):
-- writing a Variant column with a LowCardinality element to a MergeTree table.
SET allow_suspicious_variant_types = 1;
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_lc_variant_binary;
CREATE TABLE t_lc_variant_binary (v Array(Variant(LowCardinality(UInt8), String, UInt8))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_variant_binary SELECT [toLowCardinality(17), 42, 'x'];
SELECT * FROM t_lc_variant_binary;
DROP TABLE t_lc_variant_binary;
