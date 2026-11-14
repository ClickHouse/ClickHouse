-- Regression test for ColumnVariant::filter sharing variant column pointers
-- in the hasOnlyNulls() optimization path. The filter was copying ColumnPtr
-- shared pointers instead of cloning, causing two ColumnVariant objects to
-- share the same variant columns. When one was mutated via insertRangeFrom,
-- the other became inconsistent (discriminators_size=0 but variant_sizes>0),
-- leading to a LOGICAL_ERROR in compress().

SET cross_join_min_rows_to_compress = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_variant_filter;
CREATE TABLE test_variant_filter (`id` UInt64, `d` Dynamic) ENGINE = Memory;

INSERT INTO test_variant_filter SELECT number, NULL FROM numbers(50000);
INSERT INTO test_variant_filter SELECT number, [number, 'str']::Array(Variant(String, UInt64)) FROM numbers(50000);

SELECT 1 FROM test_variant_filter WHERE NOT empty((SELECT d.`Array(Variant(String, UInt64))`)) FORMAT Null;

DROP TABLE test_variant_filter;
