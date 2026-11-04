-- argMin / argMax return the first row among ties within a block

SET max_block_size = 1000000;

SELECT 'UInt8', argMin(number, toUInt8((number + 1) % 3)), argMax(number, toUInt8((number + 1) % 3)) FROM numbers(100);
SELECT 'Int32', argMin(number, toInt32((number + 1) % 3)), argMax(number, toInt32((number + 1) % 3)) FROM numbers(100);
SELECT 'Int64', argMin(number, toInt64((number + 1) % 3)), argMax(number, toInt64((number + 1) % 3)) FROM numbers(100);
SELECT 'Float32', argMin(number, toFloat32((number + 1) % 3)), argMax(number, toFloat32((number + 1) % 3)) FROM numbers(100);
SELECT 'Float64', argMin(number, toFloat64((number + 1) % 3)), argMax(number, toFloat64((number + 1) % 3)) FROM numbers(100);
SELECT 'Decimal64', argMin(number, toDecimal64((number + 1) % 3, 2)), argMax(number, toDecimal64((number + 1) % 3, 2)) FROM numbers(100);
SELECT 'DateTime64', argMin(number, toDateTime64((number + 1) % 3, 3, 'UTC')), argMax(number, toDateTime64((number + 1) % 3, 3, 'UTC')) FROM numbers(100);

SELECT 'Float64 with NaN', argMin(number, if(number % 3 = 0, nan, toFloat64((number + 1) % 3))), argMax(number, if(number % 3 = 0, nan, toFloat64((number + 1) % 3))) FROM numbers(100);
SELECT 'Float64 all NaN', argMin(number, nan), argMax(number, nan) FROM numbers(100);
SELECT 'Float64 signed zero', argMin(number, if(number = 5, -0., if(number = 7, 0., 1.))) FROM numbers(100);

-- Ties spread over many chunks
SELECT 'Int64 large', argMin(number, if(number < 20000, 5, (number + 1) % 3)), argMax(number, if(number < 20000, -5, (number + 1) % 3)) FROM numbers(100000);
SELECT 'Float64 large', argMin(number, if(number < 20000, 5., toFloat64((number + 1) % 3))), argMax(number, if(number < 20000, -5., toFloat64((number + 1) % 3))) FROM numbers(100000);

-- Same result as the -If variant, which already returned the first row
SELECT 'Int64 If', argMinIf(number, (number + 1) % 3, 1), argMaxIf(number, (number + 1) % 3, 1) FROM numbers(100);
