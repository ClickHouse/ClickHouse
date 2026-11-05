-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105294
-- maxMap / minMap and maxMappedArrays / minMappedArrays must agree on NaN
-- handling: NaN is treated as last (matches `max`/`min` after PR #100448).

SELECT 'Queries from issue #105294';
SELECT maxMappedArrays(a, b)
FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [nan]), (['y'], [6]));

SELECT maxMap(map('y', x))
FROM VALUES('x Float64', (nan), (6));

SELECT 'NaN first then a real value';
SELECT minMap(map('y', x)) FROM VALUES('x Float64', (nan), (6));
SELECT minMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [nan]), (['y'], [6]));

SELECT 'A real value first then NaN';
SELECT maxMap(map('y', x)) FROM VALUES('x Float64', (6), (nan));
SELECT maxMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [6]), (['y'], [nan]));
SELECT minMap(map('y', x)) FROM VALUES('x Float64', (6), (nan));
SELECT minMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [6]), (['y'], [nan]));

SELECT 'All NaN values keep NaN';
SELECT maxMap(map('y', x)) FROM VALUES('x Float64', (nan), (nan));
SELECT maxMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [nan]), (['y'], [nan]));
SELECT minMap(map('y', x)) FROM VALUES('x Float64', (nan), (nan));
SELECT minMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [nan]), (['y'], [nan]));

SELECT 'NaN sandwiched between real values';
SELECT maxMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [3]), (['y'], [nan]), (['y'], [6]));
SELECT minMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float64)', (['y'], [3]), (['y'], [nan]), (['y'], [6]));

SELECT 'Float32 too';
SELECT maxMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float32)', (['y'], [toFloat32(nan)]), (['y'], [toFloat32(6)]));
SELECT minMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Float32)', (['y'], [toFloat32(nan)]), (['y'], [toFloat32(6)]));

SELECT 'Integer types are unaffected';
SELECT maxMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Int32)', (['y'], [3]), (['y'], [6]));
SELECT minMappedArrays(a, b) FROM VALUES('a Array(Char), b Array(Int32)', (['y'], [3]), (['y'], [6]));

SELECT 'Merging across groups (forces serialize/deserialize path)';
SELECT maxMappedArrays(a, b) FROM (
    SELECT ['y']::Array(Char) AS a, [nan]::Array(Float64) AS b
    UNION ALL
    SELECT ['y']::Array(Char) AS a, [6]::Array(Float64) AS b
) SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
SELECT minMappedArrays(a, b) FROM (
    SELECT ['y']::Array(Char) AS a, [nan]::Array(Float64) AS b
    UNION ALL
    SELECT ['y']::Array(Char) AS a, [6]::Array(Float64) AS b
) SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
