-- Tags: no-fasttest
-- Tag no-fasttest: needs s2 (geoToS2)

-- Regression: with variant_throw_on_type_mismatch=false / dynamic_throw_on_type_mismatch=false,
-- value-dependent exceptions from compatible alternatives must still propagate.
-- geoToS2 builds successfully for Float64 but throws ILLEGAL_TYPE_OF_ARGUMENT during
-- execute when coordinates are NaN — this is a value error, not a type mismatch, so it
-- must NOT be silently converted to NULL even when throw_on_type_mismatch is disabled.
-- https://github.com/ClickHouse/ClickHouse/issues/103484

SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET allow_experimental_dynamic_type = 1;
SET variant_throw_on_type_mismatch = false;
SET dynamic_throw_on_type_mismatch = false;

-- Variant: geoToS2 with NaN longitude — compatible Float64 type, value error during execute
SELECT 'variant: value-dependent exception propagates';
DROP TABLE IF EXISTS test_v_geo;
CREATE TABLE test_v_geo (id UInt32, v Variant(Float64, String)) ENGINE = Memory;
INSERT INTO test_v_geo VALUES (1, toFloat64('nan'));
SELECT geoToS2(v, 0.0) FROM test_v_geo; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE test_v_geo;

-- Dynamic: same regression for Dynamic type
SELECT 'dynamic: value-dependent exception propagates';
DROP TABLE IF EXISTS test_d_geo;
CREATE TABLE test_d_geo (id UInt32, v Dynamic) ENGINE = Memory;
INSERT INTO test_d_geo VALUES (1, toFloat64('nan'));
SELECT geoToS2(v, 0.0) FROM test_d_geo; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE test_d_geo;
