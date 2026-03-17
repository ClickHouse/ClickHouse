-- Test default behavior (precision = 0, current dragonbox algorithm)  
SET output_format_float_precision = 0;  
SELECT 1.0/3;
SELECT 3.141592653589793;
SELECT 0.123456789012345;  
  
-- Test specific precision values  
SET output_format_float_precision = 6;  
SELECT 1.0/3;
SELECT 3.141592653589793;
SELECT 0.123456789012345; 
  
SET output_format_float_precision = 3;  
SELECT 1.0/3;
SELECT 3.141592653589793;
SELECT 0.123456789012345;

-- Test both Float32 and Float64  
SET output_format_float_precision = 4;
SELECT toFloat32(1.0/3);
SELECT toFloat64(1.0/3);
SELECT toFloat32(3.141592653589793);
SELECT toFloat64(3.141592653589793);

-- Test very large and very small numbers  
SET output_format_float_precision = 10;  
SELECT 1e10;
SELECT 1e-10;
SELECT 1.23456789012345e20;
SELECT 1.23456789012345e-20;
  
-- Test rounding behavior  
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

SET output_format_float_precision = 3;
-- Test TabSeparated  
SELECT 1.0/3 FORMAT TabSeparated; 
  
-- Test JSON  
SELECT 1.0/3 FORMAT JSONEachRow;
  
-- Test CSV  
SELECT 1.0/3 FORMAT CSV;