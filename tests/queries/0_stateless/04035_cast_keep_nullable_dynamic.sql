-- Tags: no-fasttest, no-random-settings
-- Tests for cast_keep_nullable with Dynamic/JSON types
SET enable_analyzer = 1;
SET cast_keep_nullable = 1;

-- Dynamic cast NULL to Nullable-eligible types: should return NULL
SELECT v.a::Int32 AS val, toTypeName(val) FROM (SELECT '{"a":null}'::JSON AS v);
SELECT v.a::String AS val, toTypeName(val) FROM (SELECT '{"a":null}'::JSON AS v);

-- Dynamic cast NULL to non-Nullable type (Array): should throw
SELECT v.a::Array(String) FROM (SELECT '{"a":null}'::JSON AS v); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- Dynamic cast non-NULL to Array: should work fine
SELECT v.a::Array(UInt32) FROM (SELECT '{"a":[1,2,3]}'::JSON AS v);

-- Without setting: backward compatible
SET cast_keep_nullable = 0;
SELECT v.a::Array(String) FROM (SELECT '{"a":null}'::JSON AS v);
SELECT v.a::Int32 FROM (SELECT '{"a":null}'::JSON AS v);

-- Via-Dynamic cast consistency with direct cast
SET cast_keep_nullable = 1;
SELECT vv::Array(String) FROM (SELECT v.a::LowCardinality(Nullable(String)) AS vv FROM (SELECT '{"a":null}'::JSON AS v)); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT vv::Dynamic::Array(String) FROM (SELECT v.a::LowCardinality(Nullable(String)) AS vv FROM (SELECT '{"a":null}'::JSON AS v)); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- Dynamic cast NULL to Variant: should return NULL, not throw (issue #100793)
SET allow_experimental_variant_type = 1;
SET cast_keep_nullable = 1;
SELECT v.a::Variant(String, UInt64) FROM (SELECT '{"a":null}'::JSON AS v);
