-- Subnormal (denormalized) Float64 values should be parseable as literals.
-- https://github.com/ClickHouse/ClickHouse/issues/38455

SELECT 4.9406564584124654E-324 > 0;
SELECT 1.0e-308 > 0;
SELECT 1.0e-323 > 0;
SELECT 1e-307 > 0;
SELECT toFloat64(4.9406564584124654E-324) > 0;
SELECT toFloat64(1.0e-308) > 0;
SELECT toFloat64(1.0e-323) > 0;
