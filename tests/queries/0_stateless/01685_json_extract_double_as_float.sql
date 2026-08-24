
WITH '{ "v":1.1}' AS raw
SELECT
    JSONExtract(raw, 'v', 'float') AS float32_1,
    JSONExtract(raw, 'v', 'Float32') AS float32_2,
    JSONExtractFloat(raw, 'v') AS float64_1,
    JSONExtract(raw, 'v', 'double') AS float64_2;

WITH '{ "v":1E-2}' AS raw
SELECT
    JSONExtract(raw, 'v', 'float') AS float32_1,
    JSONExtract(raw, 'v', 'Float32') AS float32_2,
    JSONExtractFloat(raw, 'v') AS float64_1,
    JSONExtract(raw, 'v', 'double') AS float64_2;

SELECT JSONExtract('{"v":1.1}', 'v', 'UInt64');
SELECT JSONExtract('{"v":1.1}', 'v', 'Nullable(UInt64)');

SELECT JSONExtract('{"v":-1e300}', 'v', 'Float64');
SELECT JSONExtract('{"v":-1e300}', 'v', 'Float32');

SELECT JSONExtract('{"v":-1e300}', 'v', 'UInt64');
SELECT JSONExtract('{"v":-1e300}', 'v', 'Int64');
SELECT JSONExtract('{"v":-1e300}', 'v', 'UInt8');
SELECT JSONExtract('{"v":-1e300}', 'v', 'Int8');
