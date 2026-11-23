-- `hierarchicalKMeans` and `assignCentroid` accept Array(Int8) and Array(UInt8), cast to Float32 as is.

SELECT '-- hierarchicalKMeans: four distinct points, so the centroids are exactly those points';
SELECT arraySort(hierarchicalKMeans(4)(v)) FROM (
    SELECT [toUInt8(intDiv(number, 25) * 80), toUInt8(255 - intDiv(number, 25) * 80)] AS v FROM numbers(100));
SELECT arraySort(hierarchicalKMeans(4)(v)) FROM (
    SELECT [toInt8(intDiv(number, 25) * 80 - 128), toInt8(127 - intDiv(number, 25) * 80)] AS v FROM numbers(100));
SELECT arrayMap(c -> arrayMap(x -> round(x, 4), c), arraySort(hierarchicalKMeans(4, 16, 20, 1000000, 0, 1)(v))) FROM (
    SELECT [toUInt8(intDiv(number, 25) * 80), toUInt8(255 - intDiv(number, 25) * 80)] AS v FROM numbers(100));

SELECT '-- assignCentroid: the vector is not shifted, so [200, 250] is nearest to [200, 250], not to [72, 122]';
SELECT assignCentroid([200, 250]::Array(UInt8), [[0.0, 0.0], [72.0, 122.0], [200.0, 250.0]]::Array(Array(Float32)));
SELECT assignCentroid([-100, 5]::Array(Int8), [[0.0, 0.0], [-100.0, 5.0], [100.0, 5.0]]::Array(Array(Float32)));
SELECT '-- integer centroid literals are accepted too';
SELECT assignCentroid([1, 2], [[0, 0], [1, 2]]);
-- `[[0, 0], [-1, -2]]` alone is Array(Array(Int16)), the supertype of UInt8 and Int8, so it is cast.
SELECT assignCentroid([-1, -2]::Array(Int8), [[0, 0], [-1, -2]]::Array(Array(Int8)));
-- UInt8 centroids above 127 are cast as is: a -128 shift would make the first query return 1, a signed read the second 0.
SELECT assignCentroid([72.0, 122.0], [[72, 122], [200, 250]]::Array(Array(UInt8)));
SELECT assignCentroid([200.0, 250.0], [[72, 122], [200, 250]]::Array(Array(UInt8)));

SELECT '-- same ids as the explicit CAST to Array(Float32), on many rows';
DROP TABLE IF EXISTS vectors_u8;
CREATE TABLE vectors_u8 (id UInt32, v Array(UInt8), w Array(Int8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO vectors_u8 SELECT number,
    arrayMap(i -> toUInt8(sipHash64(number, i) % 256), range(16)),
    arrayMap(i -> toInt8(toInt16(sipHash64(number, i + 100) % 256) - 128), range(16))
FROM numbers(1000);
WITH (SELECT hierarchicalKMeans(32)(v) FROM vectors_u8) AS cv,
     (SELECT hierarchicalKMeans(32)(w) FROM vectors_u8) AS cw
SELECT countIf(assignCentroid(v, cv) != assignCentroid(CAST(v, 'Array(Float32)'), cv)),
       countIf(assignCentroid(w, cw) != assignCentroid(CAST(w, 'Array(Float32)'), cw)),
       uniqExact(assignCentroid(v, cv)) > 10, uniqExact(assignCentroid(w, cw)) > 10
FROM vectors_u8;

SELECT '-- LowCardinality elements are accepted, as for floats';
SELECT hierarchicalKMeans(2)([1, 2]::Array(LowCardinality(UInt8))), assignCentroid([1, 2]::Array(LowCardinality(Int8)), [[0, 0], [1, 2]])
SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT '-- other integer widths and Nullable elements are still rejected';
SELECT hierarchicalKMeans(2)([1, 2]::Array(Nullable(UInt8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hierarchicalKMeans(2)([1, 2]::Array(Int16)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT assignCentroid([1, 2]::Array(Nullable(UInt8)), [[0.0, 0.0]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT assignCentroid([1.0, 2.0], [[1000, 2000]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE vectors_u8;
