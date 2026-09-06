-- Tests for the `hierarchicalKMeans` aggregate, which trains the coarse quantizer for a SQL-side IVF index.


DROP TABLE IF EXISTS blobs;

-- Four well-separated blobs, so the clustering has an unambiguous right answer.
CREATE TABLE blobs (g UInt8, v Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO blobs
SELECT number % 4 AS g,
       [toFloat32(number % 4 * 100) + toFloat32(sipHash64(number, 1) % 10) / 10,
        toFloat32(number % 4 * 100) + toFloat32(sipHash64(number, 2) % 10) / 10]::Array(Float32)
FROM numbers(20000);

SELECT '-- any float width is accepted; plain literals are Array(Float64)';
SELECT length(hierarchicalKMeans(2)([1.0, 2.0]));
SELECT length(hierarchicalKMeans(4)(v)) FROM (SELECT arrayJoin([[1.0,1.0],[2.0,2.0],[9.0,9.0],[8.0,8.0],[3.0,3.0]]) AS v);
SELECT length(hierarchicalKMeans(4)(v)) FROM (SELECT arrayJoin([[1.0,1.0],[2.0,2.0],[9.0,9.0],[8.0,8.0],[3.0,3.0]])::Array(BFloat16) AS v);

SELECT '-- returns exactly k centroids of the input dimension';
SELECT length(c), length(c[1]) FROM (SELECT hierarchicalKMeans(4)(v) AS c FROM blobs);
SELECT length(hierarchicalKMeans(64)(v)) FROM blobs;
SELECT length(hierarchicalKMeans(1000)(v)) FROM blobs;

SELECT '-- k is capped by the number of rows, since one row yields at most one centroid';
SELECT length(hierarchicalKMeans(30000)(v)) FROM blobs;

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
SELECT hierarchicalKMeans(2)(v) FROM (SELECT 1.0::Float32 AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hierarchicalKMeans(2)([1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hierarchicalKMeans(2)(['a', 'b']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Parameters themselves must be sane.
SELECT hierarchicalKMeans(-1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(1.5)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans('4')(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, -1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, -1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
-- Every parameter position goes through the same integer check, so a negative, float, string or NULL
-- value is rejected wherever it appears, not only in `k`.
SELECT hierarchicalKMeans(NULL)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, -1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, 1000000, -1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, 1000000, 1.5)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, 1000000, 0, 'x')(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, 1000000, 0, -1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
-- An empty vector would make `dim` zero, which is also the "no rows yet" sentinel.
SELECT hierarchicalKMeans(1)([]::Array(Float32)); -- { serverError BAD_ARGUMENTS }
-- No comparison against NaN is true, so a non-finite coordinate would collect rows into cluster 0 and can
-- emit non-finite centroids. Rejected on the way in, matching the rest of the vector-search stack.
SELECT hierarchicalKMeans(1)([toFloat32(nan)]::Array(Float32)); -- { serverError INCORRECT_DATA }
SELECT hierarchicalKMeans(1)([toFloat32(inf)]::Array(Float32)); -- { serverError INCORRECT_DATA }
SELECT hierarchicalKMeans(1)([-toFloat32(inf)]::Array(Float32)); -- { serverError INCORRECT_DATA }
-- Finite is not sufficient: the training math is Float32, so a coordinate whose square overflows would
-- collapse every assignment into one cluster.
SELECT hierarchicalKMeans(1)([2e19]::Array(Float32)); -- { serverError INCORRECT_DATA }
-- A state can be written by one query and read by another, so the same checks run on deserialization.
-- The float in a real state is patched from 1.0 to NaN here, so the test does not depend on the exact
-- byte layout of the state.
SELECT finalizeAggregation(CAST(
    unhex(replaceOne(hex(hierarchicalKMeansState(1, 16, 20, 100)(v)), '0000803F', '0000C07F')),
    'AggregateFunction(hierarchicalKMeans(1, 16, 20, 100), Array(Float32))'))
FROM (SELECT [1.0]::Array(Float32) AS v); -- { serverError INCORRECT_DATA }
-- Cosine is undefined for a vector with no direction.
SELECT hierarchicalKMeans(1, 16, 20, 1000000, 0, 1)([0, 0]::Array(Float32)); -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(0)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4, 16, 20, 0)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(4)(v, v) FROM blobs; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hierarchicalKMeans(4, 16, 20, 1000000, 0, 1, 999)(v) FROM blobs; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Invalid parameters are rejected, not silently clamped to something the caller did not ask for.
SELECT hierarchicalKMeans(256, 1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
SELECT hierarchicalKMeans(256, 16, 0)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
-- Row indices in the training tree are UInt32, and a reservoir past that is unreachable anyway.
SELECT hierarchicalKMeans(4, 16, 20, 5000000000)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
-- A reservoir smaller than k could never yield k centroids, so the contract is rejected up front.
SELECT hierarchicalKMeans(2, 16, 20, 1)(v) FROM blobs; -- { serverError BAD_ARGUMENTS }
-- A hand-written state must not be trusted to size an allocation: this one claims 1M vectors of
-- dimension 3 while sample_cap is 5.
SELECT finalizeAggregation(CAST(unhex('030000006400000000000000C08DB701'),
    'AggregateFunction(hierarchicalKMeans(2, 16, 20, 5), Array(Float32))')); -- { serverError INCORRECT_DATA }

-- Ragged input: all vectors must share a dimension.
SELECT hierarchicalKMeans(2)(v) FROM (SELECT arrayJoin([[1.0, 2.0], [3.0]])::Array(Float32) AS v); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE blobs;
