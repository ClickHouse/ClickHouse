-- Test: Nullable(T) -> T typed path change should not throw on NULLs,
-- NULLs should be replaced with type defaults.

-- Nullable(UInt32) -> UInt32: NULL becomes 0.
SELECT '-- Nullable(UInt32) -> UInt32';
SELECT '{"a":null}'::JSON(a Nullable(UInt32)) AS j, (j::JSON(a UInt32))."a" AS a;
SELECT '{"a":42}'::JSON(a Nullable(UInt32)) AS j, (j::JSON(a UInt32))."a" AS a;

-- Nullable(String) -> String: NULL becomes empty string.
SELECT '-- Nullable(String) -> String';
SELECT '{"a":null}'::JSON(a Nullable(String)) AS j, (j::JSON(a String))."a" AS a;
SELECT '{"a":"hello"}'::JSON(a Nullable(String)) AS j, (j::JSON(a String))."a" AS a;

-- Nullable(Float64) -> Float64: NULL becomes 0.
SELECT '-- Nullable(Float64) -> Float64';
SELECT '{"a":null}'::JSON(a Nullable(Float64)) AS j, (j::JSON(a Float64))."a" AS a;
SELECT '{"a":3.14}'::JSON(a Nullable(Float64)) AS j, (j::JSON(a Float64))."a" AS a;

-- Array(Nullable(UInt32)) -> Array(UInt32): NULLs in array become 0.
SELECT '-- Array(Nullable(UInt32)) -> Array(UInt32)';
SELECT '{"a":[1,null,3]}'::JSON(a Array(Nullable(UInt32))) AS j, (j::JSON(a Array(UInt32)))."a" AS a;
SELECT '{"a":[10,20,30]}'::JSON(a Array(Nullable(UInt32))) AS j, (j::JSON(a Array(UInt32)))."a" AS a;

-- Array(Nullable(String)) -> Array(String): NULLs become empty strings.
SELECT '-- Array(Nullable(String)) -> Array(String)';
SELECT '{"a":["hello",null,"world"]}'::JSON(a Array(Nullable(String))) AS j, (j::JSON(a Array(String)))."a" AS a;

-- Nullable(UInt32) -> Nullable(String): should still work (both nullable, no stripping needed).
SELECT '-- Nullable(UInt32) -> Nullable(String) (both nullable, no stripping)';
SELECT '{"a":null}'::JSON(a Nullable(UInt32)) AS j, (j::JSON(a Nullable(String)))."a" AS a;
SELECT '{"a":42}'::JSON(a Nullable(UInt32)) AS j, (j::JSON(a Nullable(String)))."a" AS a;
