SELECT CAST([], 'Array(Variant(UInt64, String))');
SELECT CAST(map(), 'Map(String, Variant(UInt64, String))');
SELECT CAST(tuple([]), 'Tuple(Array(Variant(UInt64, String)))');
SELECT CAST([], 'Array(Dynamic)');
SELECT CAST([42]::Array(UInt64), 'Array(Variant(UInt64, String))');
-- A `Variant` stores NULL under its own `None` discriminator, and no member may be `Nullable`.
SELECT CAST([NULL], 'Array(Variant(UInt64, String))');
SELECT CAST(map('a', NULL), 'Map(String, Variant(UInt64, String))');
SELECT CAST(map('a', 1::Nullable(UInt64)), 'Map(String, Variant(UInt64, String))');
SELECT CAST(42::Int8, 'Variant(UInt64, String)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(tuple(), 'Variant(UInt64, String)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST([], 'Array(Variant(UInt64, String))') FROM remote('127.0.0.2', system.one);
