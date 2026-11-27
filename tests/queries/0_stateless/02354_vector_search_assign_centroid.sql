-- Tests for `assignCentroid`, which routes a vector to its nearest (L2) centroid for a SQL-side IVF index.


SELECT '-- inline centroids: the id is the position in the array';
SELECT assignCentroid([1.0, 2.0]::Array(Float32), [[0.0, 0.0], [1.0, 2.0], [5.0, 5.0]]::Array(Array(Float32)));
SELECT assignCentroid([9.0, 9.0]::Array(Float32), [[0.0, 0.0], [1.0, 2.0], [5.0, 5.0]]::Array(Array(Float32)));
SELECT assignCentroid([0.1, 0.1]::Array(Float32), [[0.0, 0.0], [1.0, 2.0], [5.0, 5.0]]::Array(Array(Float32)));

SELECT '-- any float width is accepted; plain literals are Array(Float64)';
SELECT assignCentroid([1.0, 2.0], [[0.0, 0.0], [1.0, 2.0], [5.0, 5.0]]);
SELECT assignCentroid([1.0, 2.0]::Array(BFloat16), [[0.0, 0.0], [1.0, 2.0], [5.0, 5.0]]::Array(Array(Float32)));
SELECT assignCentroid([9.0, 9.0], [[0.0, 0.0], [1.0, 2.0], [5.0, 5.0]]::Array(Array(Float32)));

SELECT '-- a single centroid captures everything';
SELECT assignCentroid([42.0, -7.0]::Array(Float32), [[0.0, 0.0]]::Array(Array(Float32)));

SELECT '-- ties resolve to the lowest id, so identical centroids never split a group';
SELECT assignCentroid([1.0, 1.0]::Array(Float32), [[1.0, 1.0], [1.0, 1.0], [1.0, 1.0]]::Array(Array(Float32)));

DROP TABLE IF EXISTS centroids;
DROP TABLE IF EXISTS probes;
DROP DICTIONARY IF EXISTS centroids_dict;

CREATE TABLE centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO centroids
SELECT number, arrayMap(i -> toFloat32(sipHash64(number, i) % 997) / 997, range(37))::Array(Float32)
FROM numbers(301);

CREATE DICTIONARY centroids_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 100000)) LIFETIME(0);

CREATE TABLE probes (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO probes
SELECT number, arrayMap(i -> toFloat32(sipHash64(number + 31, i) % 997) / 997, range(37))::Array(Float32)
FROM numbers(1003);

SELECT '-- the scoring kernel matches a brute-force argmin exactly';
-- The kernel scores 16 centroids against 6 vectors at a time. 301 and 1003 divide evenly by neither, so
-- this covers the leftover centroids and the leftover rows as well as the full blocks.
-- A CROSS JOIN rather than a correlated subquery: the old analyzer cannot resolve an outer column inside
-- a subquery, and this test runs under both analyzers.
SELECT countIf(fast != brute), count() FROM (
    SELECT p.id AS id, any(p.fast) AS fast, argMin(c.cid, L2Distance(c.vec, p.v)) AS brute
    FROM (SELECT id, v, assignCentroid(v, 'centroids_dict') AS fast FROM probes) AS p
    CROSS JOIN centroids AS c
    GROUP BY p.id
);

SELECT '-- 1 and 7 rows, either side of the 6-row block, also agree with brute force';
SELECT countIf(fast != brute) FROM (
    SELECT p.id AS id, any(p.fast) AS fast, argMin(c.cid, L2Distance(c.vec, p.v)) AS brute
    FROM (SELECT id, v, assignCentroid(v, 'centroids_dict') AS fast
          FROM (SELECT id, v FROM probes ORDER BY id LIMIT 1)) AS p
    CROSS JOIN centroids AS c
    GROUP BY p.id
);
SELECT countIf(fast != brute) FROM (
    SELECT p.id AS id, any(p.fast) AS fast, argMin(c.cid, L2Distance(c.vec, p.v)) AS brute
    FROM (SELECT id, v, assignCentroid(v, 'centroids_dict') AS fast
          FROM (SELECT id, v FROM probes ORDER BY id LIMIT 7)) AS p
    CROSS JOIN centroids AS c
    GROUP BY p.id
);

SELECT '-- the dictionary form returns the dictionary cid, not the row position';
SELECT assignCentroid((SELECT vec FROM centroids WHERE cid = 250), 'centroids_dict');

SELECT '-- inline and dictionary forms agree when the ids line up';
SELECT countIf(a != b) FROM (
    SELECT assignCentroid(v, 'centroids_dict') AS a,
           assignCentroid(v, (SELECT groupArray(vec) FROM (SELECT vec FROM centroids ORDER BY cid))) AS b
    FROM (SELECT v FROM probes ORDER BY id LIMIT 50)
);

SELECT '-- the cached centroid matrix is invalidated by a dictionary reload';
-- The cache lives on the function object and must not be keyed on a raw dictionary address: a reloaded
-- dictionary can land at the address the old one had, which would silently serve stale centroids.
DROP TABLE IF EXISTS reload_src;
DROP DICTIONARY IF EXISTS reload_dict;
CREATE TABLE reload_src (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO reload_src VALUES (0, [0.0]), (1, [100.0]);
CREATE DICTIONARY reload_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'reload_src')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);
SELECT assignCentroid([99.0]::Array(Float32), 'reload_dict');
TRUNCATE TABLE reload_src;
INSERT INTO reload_src VALUES (0, [99.0]), (1, [-100.0]);
SYSTEM RELOAD DICTIONARY reload_dict;
SELECT assignCentroid([99.0]::Array(Float32), 'reload_dict');
DROP DICTIONARY reload_dict;
DROP TABLE reload_src;

SELECT '-- errors';
SELECT assignCentroid([1.0, 2.0]::Array(Float32)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT assignCentroid(1.0::Float32, [[0.0]]::Array(Array(Float32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT assignCentroid([1, 2], [[0, 0], [1, 2]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT assignCentroid(['a'], [[0.0]]::Array(Array(Float32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- The vector and the centroids must agree on dimension.
SELECT assignCentroid([1.0, 2.0, 3.0]::Array(Float32), [[0.0, 0.0]]::Array(Array(Float32))); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT assignCentroid([1.0, 2.0]::Array(Float32), []::Array(Array(Float32))); -- { serverError BAD_ARGUMENTS }
-- Non-finite input is rejected rather than silently mapped. `score < bs` is false for NaN, so a NaN probe
-- would otherwise fall through to the first id and look like a legitimate answer, and a NaN centroid would
-- be quietly unreachable.
SELECT assignCentroid([toFloat32(nan)]::Array(Float32), [[0.0], [1.0]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
SELECT assignCentroid([toFloat32(inf)]::Array(Float32), [[0.0], [1.0]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
SELECT assignCentroid([-toFloat32(inf)]::Array(Float32), [[0.0], [1.0]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
SELECT assignCentroid([5.0]::Array(Float32), [[toFloat32(nan)], [1.0]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
SELECT assignCentroid([5.0]::Array(Float32), [[toFloat32(inf)], [1.0]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
-- Finite is not sufficient: the scoring math is Float32, so a coordinate whose square overflows would make
-- every comparison false and silently return the fallback id. Bounded on both arguments instead.
SELECT assignCentroid([2e19]::Array(Float32), [[0.0], [2e19]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
SELECT assignCentroid([1.0]::Array(Float32), [[0.0], [2e19]]::Array(Array(Float32))); -- { serverError INCORRECT_DATA }
-- Well below the bound, so still accepted.
SELECT assignCentroid([1e15]::Array(Float32), [[0.0], [1e15]]::Array(Array(Float32)));
-- The centroid argument has to be constant, so the matrix is built once per block rather than per row.
SELECT assignCentroid(v, materialize([[0.0, 0.0]]::Array(Array(Float32)))) FROM (SELECT [1.0, 2.0]::Array(Float32) AS v); -- { serverError ILLEGAL_COLUMN }

DROP DICTIONARY centroids_dict;
DROP TABLE probes;
DROP TABLE centroids;
