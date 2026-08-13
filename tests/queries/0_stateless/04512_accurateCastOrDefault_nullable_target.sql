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
