-- When the target type is Nullable, accurateCastOrDefault should return NULL on
-- cast failure, not the inner type's default value.
SELECT accurateCastOrDefault('test', 'Nullable(Bool)');
SELECT accurateCastOrDefault('not_a_number', 'Nullable(UInt32)');
SELECT accurateCastOrDefault('bad', 'Nullable(Int64)');
SELECT accurateCastOrDefault('bad', 'Nullable(Float64)');
SELECT accurateCastOrDefault('bad', 'Nullable(Date)');

-- Successful casts should return the actual value.
SELECT accurateCastOrDefault('1', 'Nullable(Bool)');
SELECT accurateCastOrDefault('123', 'Nullable(UInt32)');

-- NULL input should produce NULL output for Nullable targets.
SELECT accurateCastOrDefault(NULL, 'Nullable(UInt32)');

-- A NULL input is a successful cast to a Nullable target, not a failure that
-- should be replaced with an explicit default.
SELECT accurateCastOrDefault(NULL, 'Nullable(UInt32)', CAST(42, 'Nullable(UInt32)'));

-- Dynamic and Variant encode NULL with a discriminator rather than a physical
-- null map, but it is still a successful cast to a Nullable target.
SELECT accurateCastOrDefault(CAST(NULL, 'Dynamic'), 'Nullable(UInt32)', CAST(42, 'Nullable(UInt32)'));
SELECT accurateCastOrDefault(CAST(NULL, 'Variant(UInt8, String, Nothing)'), 'Nullable(UInt32)', CAST(42, 'Nullable(UInt32)'));

-- `Dynamic` and `Variant` source NULLs are preserved for non-Nullable targets
-- when `cast_keep_nullable` is enabled, just like physically Nullable sources.
SET cast_keep_nullable = 1;
SELECT accurateCastOrDefault(CAST(NULL, 'Dynamic'), 'UInt32');
SELECT accurateCastOrDefault(CAST(NULL, 'Variant(UInt8, String, Nothing)'), 'UInt32');
SELECT toUInt32OrDefault(CAST(NULL, 'Dynamic'));
SELECT toUInt32OrDefault(CAST(NULL, 'Variant(UInt8, String, Nothing)'));
