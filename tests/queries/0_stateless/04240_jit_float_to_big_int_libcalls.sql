-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- https://github.com/ClickHouse/ClickHouse/issues/105031

SELECT '--- JIT ---';
SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '--- Float -> 128-bit / 256-bit integers ---';
SELECT toInt128 (materialize(1.5) + materialize(0.5));
SELECT toUInt128(materialize(1.5) + materialize(0.5));
SELECT toInt256 (materialize(1.5) + materialize(0.5));
SELECT toUInt256(materialize(1.5) + materialize(0.5));
SELECT toInt128 (materialize(1.5)::Float32 + materialize(0.5)::Float32);
SELECT toUInt128(materialize(1.5)::Float32 + materialize(0.5)::Float32);
SELECT toInt256 (materialize(1.5)::Float32 + materialize(0.5)::Float32);
SELECT toUInt256(materialize(1.5)::Float32 + materialize(0.5)::Float32);

SELECT '--- 128-bit / 256-bit integers -> Float ---';
SELECT toFloat32(materialize(2::Int128)  + materialize(0::Int128));
SELECT toFloat64(materialize(2::Int128)  + materialize(0::Int128));
SELECT toFloat32(materialize(2::UInt128) + materialize(0::UInt128));
SELECT toFloat64(materialize(2::UInt128) + materialize(0::UInt128));
SELECT toFloat32(materialize(2::Int256)  + materialize(0::Int256));
SELECT toFloat64(materialize(2::Int256)  + materialize(0::Int256));
SELECT toFloat32(materialize(2::UInt256) + materialize(0::UInt256));
SELECT toFloat64(materialize(2::UInt256) + materialize(0::UInt256));

SELECT '--- no JIT ---';
SET compile_expressions = 0;

SELECT '--- Float -> 128-bit / 256-bit integers ---';
SELECT toInt128 (materialize(1.5) + materialize(0.5));
SELECT toUInt128(materialize(1.5) + materialize(0.5));
SELECT toInt256 (materialize(1.5) + materialize(0.5));
SELECT toUInt256(materialize(1.5) + materialize(0.5));
SELECT toInt128 (materialize(1.5)::Float32 + materialize(0.5)::Float32);
SELECT toUInt128(materialize(1.5)::Float32 + materialize(0.5)::Float32);
SELECT toInt256 (materialize(1.5)::Float32 + materialize(0.5)::Float32);
SELECT toUInt256(materialize(1.5)::Float32 + materialize(0.5)::Float32);

SELECT '--- 128-bit / 256-bit integers -> Float ---';
SELECT toFloat32(materialize(2::Int128)  + materialize(0::Int128));
SELECT toFloat64(materialize(2::Int128)  + materialize(0::Int128));
SELECT toFloat32(materialize(2::UInt128) + materialize(0::UInt128));
SELECT toFloat64(materialize(2::UInt128) + materialize(0::UInt128));
SELECT toFloat32(materialize(2::Int256)  + materialize(0::Int256));
SELECT toFloat64(materialize(2::Int256)  + materialize(0::Int256));
SELECT toFloat32(materialize(2::UInt256) + materialize(0::UInt256));
SELECT toFloat64(materialize(2::UInt256) + materialize(0::UInt256));
