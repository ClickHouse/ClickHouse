-- Test: exercises BASIC discriminators serialization mode write path for all-NULL Variant column
-- Covers: src/DataTypes/Serializations/SerializationVariant.cpp:380-385 — BASIC mode + col.hasOnlyNulls() branch
-- The branch writes NULL_DISCRIMINATOR `limit` times to the discriminator stream when the column
-- is all-NULL and the legacy `use_compact_variant_discriminators_serialization=0` setting is used.
-- Existing tests with this setting (03260, 03411) use non-NULL inserts only.

SET allow_experimental_variant_type=1;

DROP TABLE IF EXISTS test_var_basic_nulls_wide;
DROP TABLE IF EXISTS test_var_basic_nulls_compact;

-- Wide part variant (separate streams per column)
CREATE TABLE test_var_basic_nulls_wide (id UInt64, v Variant(UInt64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS use_compact_variant_discriminators_serialization=0,
         min_rows_for_wide_part=1, min_bytes_for_wide_part=1,
         index_granularity=8192;

-- Insert ONLY-NULL Variant data: triggers col.hasOnlyNulls()==true branch
INSERT INTO test_var_basic_nulls_wide SELECT number, NULL FROM numbers(50);

-- Read the data back: verifies BASIC-mode all-NULL write produced NULL_DISCRIMINATOR (255), not 0
SELECT count() FROM test_var_basic_nulls_wide WHERE v IS NULL SETTINGS max_threads=1;
SELECT count() FROM test_var_basic_nulls_wide WHERE v.UInt64 IS NULL SETTINGS max_threads=1;
SELECT count() FROM test_var_basic_nulls_wide WHERE v.String IS NULL SETTINGS max_threads=1;
-- Subcolumn reads from BASIC-mode-written part with all-NULL discriminators
SELECT sum(v.UInt64) IS NULL FROM test_var_basic_nulls_wide SETTINGS max_threads=1;
SELECT max(v.String) IS NULL FROM test_var_basic_nulls_wide SETTINGS max_threads=1;

-- Compact part variant (single stream with marks per substream)
CREATE TABLE test_var_basic_nulls_compact (id UInt64, v Variant(UInt64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS use_compact_variant_discriminators_serialization=0,
         min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=1000000000,
         index_granularity=8192;

INSERT INTO test_var_basic_nulls_compact SELECT number, NULL FROM numbers(50);

SELECT count() FROM test_var_basic_nulls_compact WHERE v IS NULL SETTINGS max_threads=1;
SELECT count() FROM test_var_basic_nulls_compact WHERE v.UInt64 IS NULL SETTINGS max_threads=1;
SELECT count() FROM test_var_basic_nulls_compact WHERE v.String IS NULL SETTINGS max_threads=1;

DROP TABLE test_var_basic_nulls_wide;
DROP TABLE test_var_basic_nulls_compact;
