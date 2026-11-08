-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- https://github.com/ClickHouse/ClickHouse/issues/105031

SELECT '--- JIT ---';
SET compile_expressions = 1, min_count_to_compile_expression = 0;

-- Only the 128-bit integer to float direction is compiled, and it reaches `__floattisf` and
-- `__floattidf`. A float source with an integer destination is declined, and a 256-bit value is not
-- a native JIT type, so both have an interpreted arm only: the first is covered by
-- `05055_jit_float_to_integer_cast`, the second by the block below.
SELECT '--- 128-bit integers -> Float ---';
SELECT toFloat32(materialize(2::Int128)  + materialize(0::Int128));
SELECT toFloat64(materialize(2::Int128)  + materialize(0::Int128));
SELECT toFloat32(materialize(2::UInt128) + materialize(0::UInt128));
SELECT toFloat64(materialize(2::UInt128) + materialize(0::UInt128));

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

-- Every row above compares a compiled value against an interpreted one, so all of them would still
-- pass if the JIT arm stopped compiling. This pins that the 128-bit conversion is compiled wherever
-- the control is, which also holds in a build without the embedded compiler. The control is plain
-- arithmetic, so it stays at 1 even if every conversion stops being compilable.
SELECT toFloat64(materialize(2::Int128) + materialize(0::Int128))
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '04240_int128' FORMAT Null;
SELECT materialize(2.0) + materialize(0.0) + materialize(1.0)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '04240_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04240_%'
    GROUP BY log_comment
)
SELECT (SELECT compiled FROM shapes WHERE log_comment = '04240_int128')
     = (SELECT compiled FROM shapes WHERE log_comment = '04240_control');
