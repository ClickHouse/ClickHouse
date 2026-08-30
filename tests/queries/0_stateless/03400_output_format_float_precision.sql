SELECT '-- Test default behavior (precision = 0, dragonbox algorithm)';
SET output_format_float_precision = 0;
SELECT 1.0/3;
SELECT 3.141592653589793;
SELECT 0.123456789012345;

SELECT '-- Test specific precision values';
SET output_format_float_precision = 6;
SELECT 1.0/3;
SELECT 3.141592653589793;
SELECT 0.123456789012345;

SET output_format_float_precision = 3;
SELECT 1.0/3;
SELECT 3.141592653589793;
SELECT 0.123456789012345;

SELECT '-- Test both Float32 and Float64';
SET output_format_float_precision = 4;
SELECT toFloat32(1.0/3);
SELECT toFloat64(1.0/3);
SELECT toFloat32(3.141592653589793);
SELECT toFloat64(3.141592653589793);

SELECT '-- Test BFloat16';
SET output_format_float_precision = 4;
SELECT toBFloat16(1.0/3);
SELECT toBFloat16(3.141592653589793);

SELECT '-- Test trailing zeros are stripped (at most N decimal places)';
SET output_format_float_precision = 10;
SELECT toFloat64(1.0);
SELECT toFloat64(1.5);
SELECT toFloat32(1.0);

SELECT '-- Test very large and very small numbers';
SET output_format_float_precision = 10;
SELECT 1e10;
SELECT 1e-10;
SELECT 1.23456789012345e20;
SELECT 1.23456789012345e-20;

SELECT '-- Test large-magnitude numbers fall back to shortest representation (no exception)';
SET output_format_float_precision = 1;
SELECT 1e61;
SELECT -1e62;

SELECT '-- Test rounding behavior';
SET output_format_float_precision = 2;
SELECT 1.235;
SELECT 1.234;
SELECT 1.236;
SELECT -1.235;
SELECT -1.234;
SELECT -1.236;

SET output_format_float_precision = 4;
SELECT 1.234567;
SELECT toDecimalString(1.234567, 4);

SELECT '-- Test TabSeparated';
SET output_format_float_precision = 3;
SELECT 1.0/3 FORMAT TabSeparated;

SELECT '-- Test JSON';
SELECT 1.0/3 FORMAT JSONEachRow;

SELECT '-- Test CSV';
SELECT 1.0/3 FORMAT CSV;

SELECT '-- Test out-of-range precision raises a BAD_ARGUMENTS exception';
SET output_format_float_precision = 101;
SELECT 1.0/3; -- {clientError BAD_ARGUMENTS}

SELECT '-- Test non-finite values with non-zero precision (should not throw)';
SET output_format_float_precision = 5;
SELECT toFloat64('inf');
SELECT toFloat64('-inf');
SELECT toFloat64('nan');
SELECT toFloat32('inf');
SELECT toFloat32('-inf');
SELECT toFloat32('nan');
SELECT toBFloat16('inf');
SELECT toBFloat16('-inf');
SELECT toBFloat16('nan');

SELECT '-- Regression: BFloat16 trailing zeros are stripped even at high precision';
SET output_format_float_precision = 30;
SELECT (1.1::BFloat16);

SELECT '-- Regression: negative zero with precision does not pad unnecessary decimal zeros';
SET output_format_float_precision = 1;
SELECT (-0.0::BFloat16);

SELECT '-- Regression: default repr already satisfies precision, no ToFixed artefacts';
SELECT 'toFloat32(0.1) with precision=10 should give 0.1, not 0.1000000015';
SET output_format_float_precision = 10;
SELECT toFloat32(0.1);
SELECT 'toFloat32(1.23e20) with precision=1 should give the round-trip integer, not the exact float bits';
SET output_format_float_precision = 1;
SELECT toFloat32(1.23e20);
