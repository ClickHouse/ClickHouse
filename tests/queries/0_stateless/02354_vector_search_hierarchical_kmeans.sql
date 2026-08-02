-- Tests for the `hierarchicalKMeans` aggregate, which trains the coarse quantizer for a SQL-side IVF index.


DROP TABLE IF EXISTS blobs;

-- Four well-separated blobs, so the clustering has an unambiguous right answer.
CREATE TABLE blobs (g UInt8, v Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO blobs
SELECT number % 4 AS g,
       [toFloat32(number % 4 * 100) + toFloat32(sipHash64(number, 1) % 10) / 10,
        toFloat32(number % 4 * 100) + toFloat32(sipHash64(number, 2) % 10) / 10]::Array(Float32)
FROM numbers(20000);

SELECT '-- returns exactly k centroids of the input dimension';
SELECT length(c), length(c[1]) FROM (SELECT hierarchicalKMeans(4)(v) AS c FROM blobs);
SELECT length(hierarchicalKMeans(64)(v)) FROM blobs;
SELECT length(hierarchicalKMeans(1000)(v)) FROM blobs;

SELECT '-- k is capped by the number of points, since one point yields at most one centroid';
SELECT length(hierarchicalKMeans(500)(v)) FROM (SELECT [toFloat32(number), 2.0]::Array(Float32) AS v FROM numbers(100));

SELECT '-- each blob maps to exactly one cluster';
SELECT g, uniqExact(cid) FROM (
    SELECT g, assignCentroid(v, (SELECT hierarchicalKMeans(4)(v) FROM blobs)) AS cid FROM blobs
) GROUP BY g ORDER BY g;

SELECT '-- deterministic for a given seed, and a different seed is allowed to differ';
SELECT
    (SELECT hierarchicalKMeans(8, 16, 20, 1000000, 42)(v) FROM blobs)
        = (SELECT hierarchicalKMeans(8, 16, 20, 1000000, 42)(v) FROM blobs) AS same_seed_stable;

SELECT '-- spherical = 1 makes every centroid unit length';
-- Own data rather than `blobs`: that table contains exact [0, 0] rows, which spherical mode rejects.
SELECT round(min(n), 5), round(max(n), 5) FROM (
    SELECT arrayJoin(arrayMap(x -> sqrt(arraySum(y -> y * y, x)),
        (SELECT hierarchicalKMeans(16, 16, 20, 1000000, 0, 1)(v) FROM (
            SELECT [toFloat32(sipHash64(number, 1) % 100 + 1) / 100,
                    toFloat32(sipHash64(number, 2) % 100 + 1) / 100]::Array(Float32) AS v
            FROM numbers(20000))))) AS n
);

SELECT '-- degenerate input still terminates and returns k';
SELECT length(hierarchicalKMeans(50)(v)) FROM (SELECT [1.0, 2.0, 3.0]::Array(Float32) AS v FROM numbers(5000));
SELECT length(hierarchicalKMeans(200)(v)) FROM (SELECT [toFloat32(number % 3), 1.0]::Array(Float32) AS v FROM numbers(10000));

SELECT '-- empty input produces no centroids';
SELECT length(hierarchicalKMeans(4)(v)) FROM (SELECT [1.0, 2.0]::Array(Float32) AS v FROM numbers(0));

SELECT '-- the aggregate state round-trips through serialization';
SELECT length(hierarchicalKMeansMerge(4)(st)) FROM (
    SELECT hierarchicalKMeansState(4)(v) AS st FROM blobs GROUP BY g
);

SELECT '-- errors';
-- Float32 exactly: the kernel reads the nested column as ColumnFloat32, so a wider float would be
-- reinterpreted rather than converted.
SELECT hierarchicalKMeans(2)([1.0, 2.0]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hierarchicalKMeans(2)(v) FROM (SELECT [1.0, 2.0]::Array(Float64) AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hierarchicalKMeans(2)(v) FROM (SELECT 1.0::Float32 AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- An empty vector would make `dim` zero, which is also the "no rows yet" sentinel.
SELECT hierarchicalKMeans(1)([]::Array(Float32)); -- { serverError BAD_ARGUMENTS }
-- Cosine is undefined for a vector with no direction.
SELECT hierarchicalKMeans(1, 16, 20, 1000000, 0, 1)([0, 0]::Array(Float32)); -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(0)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, 0)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans()(v) FROM blobs; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hierarchicalKMeans(4)(v, v) FROM blobs; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Ragged input: all vectors must share a dimension.
SELECT hierarchicalKMeans(2)(v) FROM (SELECT arrayJoin([[1.0, 2.0], [3.0]])::Array(Float32) AS v); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE blobs;
