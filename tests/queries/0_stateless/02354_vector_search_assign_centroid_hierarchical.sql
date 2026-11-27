-- Tests for the two-level search of `assignCentroid` over a dictionary with `level` and `parent` columns:
-- every coarse (level 1) centroid is scored, then only the leaves (level 0) of the 16 nearest ones.

DROP DICTIONARY IF EXISTS h_dict;
DROP TABLE IF EXISTS coarse_src;
DROP TABLE IF EXISTS leaf_src;
DROP TABLE IF EXISTS h_src;
DROP TABLE IF EXISTS probes;
DROP TABLE IF EXISTS expected;

-- 37 coarse centroids (more than 16 and not a multiple of 16), 1000 leaves, each leaf under its nearest
-- coarse centroid: the IVF build path.
CREATE TABLE coarse_src (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO coarse_src
SELECT 1000000 + number, arrayMap(i -> toFloat32(sipHash64(number, i) % 997) / 997, range(13))::Array(Float32)
FROM numbers(37);

CREATE TABLE leaf_src (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO leaf_src
SELECT number, arrayMap(i -> toFloat32(sipHash64(number + 100, i) % 997) / 997, range(13))::Array(Float32)
FROM numbers(1000);

CREATE TABLE h_src (cid UInt64, vec Array(Float32), level UInt8, parent UInt64) ENGINE = MergeTree ORDER BY cid;
INSERT INTO h_src SELECT cid, vec, 1, 0 FROM coarse_src;
INSERT INTO h_src
SELECT cid, vec, 0, 1000000 + assignCentroid(vec, (SELECT groupArray(vec) FROM (SELECT vec FROM coarse_src ORDER BY cid)))
FROM leaf_src;

CREATE DICTIONARY h_dict (cid UInt64, vec Array(Float32), level UInt8, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'h_src')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE probes (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO probes
SELECT number, arrayMap(i -> toFloat32(sipHash64(number + 5000, i) % 997) / 997, range(13))::Array(Float32)
FROM numbers(300);

-- Brute force restricted to what the search scans: the nearest leaf whose parent is one of the 16 coarse
-- centroids with leaves nearest to the probe, ties going to the lower coarse cid.
CREATE TABLE expected (id UInt32, leaf UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO expected
SELECT p.id, argMin(l.cid, L2Distance(l.vec, p.v))
FROM
(
    SELECT p.id AS id, any(p.v) AS v,
           arraySlice(arrayMap(x -> x.2, arraySort(groupArray((L2Distance(c.vec, p.v), c.cid)))), 1, 16) AS probed
    FROM probes AS p
    CROSS JOIN (SELECT cid, vec FROM h_src WHERE level = 1 AND cid IN (SELECT parent FROM h_src WHERE level = 0)) AS c
    GROUP BY p.id
) AS p
CROSS JOIN (SELECT cid, vec, parent FROM h_src WHERE level = 0) AS l
WHERE has(p.probed, l.parent)
GROUP BY p.id;

SELECT '-- the search matches the restricted brute force exactly';
SELECT countIf(fast != leaf), count()
FROM (SELECT id, assignCentroid(v, 'h_dict') AS fast FROM probes) AS p
INNER JOIN expected AS e USING (id);

SELECT '-- 1 and 7 rows, either side of the 6-row block, also agree';
SELECT countIf(fast != leaf), count()
FROM (SELECT id, assignCentroid(v, 'h_dict') AS fast FROM (SELECT id, v FROM probes ORDER BY id LIMIT 1)) AS p
INNER JOIN expected AS e USING (id);
SELECT countIf(fast != leaf), count()
FROM (SELECT id, assignCentroid(v, 'h_dict') AS fast FROM (SELECT id, v FROM probes ORDER BY id LIMIT 7)) AS p
INNER JOIN expected AS e USING (id);

SELECT '-- a probe equal to a leaf returns that leaf';
SELECT countIf(assignCentroid(vec, 'h_dict') != cid), count() FROM leaf_src;

SELECT '-- coarse centroids are never returned';
SELECT countIf(dictGet('h_dict', 'level', toUInt64(assignCentroid(vec, 'h_dict'))) != 0), count() FROM coarse_src;

SELECT '-- with at most 16 coarse centroids every leaf is scanned, so the search is exact';
DROP DICTIONARY IF EXISTS h10_dict;
DROP TABLE IF EXISTS h10_src;
CREATE TABLE h10_src (cid UInt64, vec Array(Float32), level UInt8, parent UInt64) ENGINE = MergeTree ORDER BY cid;
INSERT INTO h10_src SELECT cid, vec, 1, 0 FROM coarse_src WHERE cid < 1000010;
INSERT INTO h10_src
SELECT cid, vec, 0,
       1000000 + assignCentroid(vec, (SELECT groupArray(vec) FROM (SELECT vec FROM coarse_src WHERE cid < 1000010 ORDER BY cid)))
FROM leaf_src WHERE cid < 400;
CREATE DICTIONARY h10_dict (cid UInt64, vec Array(Float32), level UInt8, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'h10_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT countIf(fast != brute), count() FROM (
    SELECT p.id AS id, any(p.fast) AS fast, argMin(l.cid, L2Distance(l.vec, p.v)) AS brute
    FROM (SELECT id, v, assignCentroid(v, 'h10_dict') AS fast FROM probes) AS p
    CROSS JOIN (SELECT cid, vec FROM h10_src WHERE level = 0) AS l
    GROUP BY p.id
);
DROP DICTIONARY h10_dict;
DROP TABLE h10_src;

-- Small one-dimensional dictionaries below: no distance ties, so every expected id is computed by hand.
-- Coarse centroids 101..117 sit at 1..17, each with one leaf at its own position (leaf cid = position).
-- Coarse centroid 118 sits at 100, and its only leaf (cid 18) sits at 0.5.
DROP DICTIONARY IF EXISTS approx_dict;
DROP DICTIONARY IF EXISTS approx_flat_dict;
DROP TABLE IF EXISTS approx_src;
CREATE TABLE approx_src (cid UInt64, vec Array(Float32), level UInt8, parent UInt64) ENGINE = MergeTree ORDER BY cid;
INSERT INTO approx_src SELECT 100 + number, [toFloat32(number)], 1, 0 FROM numbers(1, 17);
INSERT INTO approx_src SELECT number, [toFloat32(number)], 0, 100 + number FROM numbers(1, 17);
INSERT INTO approx_src VALUES (118, [100], 1, 0), (18, [0.5], 0, 118);
CREATE DICTIONARY approx_dict (cid UInt64, vec Array(Float32), level UInt8, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'approx_src')) LAYOUT(HASHED()) LIFETIME(0);
CREATE DICTIONARY approx_flat_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'approx_src' WHERE 'level = 0')) LAYOUT(HASHED()) LIFETIME(0);

SELECT '-- the search is approximate: the nearest leaf (18) is under the 18th nearest coarse centroid';
SELECT assignCentroid([0]::Array(Float32), 'approx_dict');
SELECT '-- the same leaves without level and parent are searched exhaustively';
SELECT assignCentroid([0]::Array(Float32), 'approx_flat_dict');
DROP DICTIONARY approx_flat_dict;

SELECT '-- the cached structure is rebuilt on reload: leaf 18 moved under coarse centroid 101';
TRUNCATE TABLE approx_src;
INSERT INTO approx_src SELECT 100 + number, [toFloat32(number)], 1, 0 FROM numbers(1, 17);
INSERT INTO approx_src SELECT number, [toFloat32(number)], 0, 100 + number FROM numbers(1, 17);
INSERT INTO approx_src VALUES (118, [100], 1, 0), (18, [0.5], 0, 101);
SYSTEM RELOAD DICTIONARY approx_dict;
SELECT assignCentroid([0]::Array(Float32), 'approx_dict');
DROP DICTIONARY approx_dict;
DROP TABLE approx_src;

SELECT '-- coarse centroids without leaves are ignored, so all 16 probes go to coarse centroids with leaves';
-- Childless coarse centroids 201..217 sit at 1..17; coarse centroids 300..316 sit at 100..116, each with
-- one leaf at its own position (leaf cid = position).
DROP DICTIONARY IF EXISTS childless_dict;
DROP TABLE IF EXISTS childless_src;
CREATE TABLE childless_src (cid UInt64, vec Array(Float32), level UInt8, parent UInt64) ENGINE = MergeTree ORDER BY cid;
INSERT INTO childless_src SELECT 200 + number, [toFloat32(number)], 1, 0 FROM numbers(1, 17);
INSERT INTO childless_src SELECT 200 + number, [toFloat32(number)], 1, 0 FROM numbers(100, 17);
INSERT INTO childless_src SELECT number, [toFloat32(number)], 0, 200 + number FROM numbers(100, 17);
CREATE DICTIONARY childless_dict (cid UInt64, vec Array(Float32), level UInt8, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'childless_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT assignCentroid([0]::Array(Float32), 'childless_dict');
SELECT '-- and with at most 16 coarse centroids with leaves, all of their leaves are scanned';
TRUNCATE TABLE childless_src;
INSERT INTO childless_src SELECT 200 + number, [toFloat32(number)], 1, 0 FROM numbers(1, 17);
INSERT INTO childless_src VALUES (300, [100], 1, 0), (350, [50], 1, 0), (400, [200], 1, 0);
INSERT INTO childless_src VALUES (100, [100], 0, 300), (50, [50], 0, 350), (200, [200], 0, 400);
SYSTEM RELOAD DICTIONARY childless_dict;
SELECT assignCentroid([0]::Array(Float32), 'childless_dict'), assignCentroid([180]::Array(Float32), 'childless_dict');
DROP DICTIONARY childless_dict;
DROP TABLE childless_src;

SELECT '-- coarse ties at the 16th place go to the lower coarse cids, for every row of a block';
-- Coarse centroids 1000..1023 at 1, 2 or 3, as the digits below give: 8 nearer ones, 9 tied ones, 7 farther ones.
-- Leaf 2000 + k sits at 10 + k under coarse centroid 1000 + k, except leaf 2017, the nearest to 0, which sits
-- at 0.1 under coarse centroid 1017, the 9th tied one. The 16 probes are the 8 nearer and the 8 lowest tied ones.
DROP DICTIONARY IF EXISTS tie_dict;
DROP TABLE IF EXISTS tie_src;
CREATE TABLE tie_src (cid UInt64, vec Array(Float32), level UInt8, parent UInt64) ENGINE = MergeTree ORDER BY cid;
INSERT INTO tie_src
SELECT 1000 + number, [toFloat32(substring('223213332122121122333111', number + 1, 1))], 1, 0 FROM numbers(24);
INSERT INTO tie_src SELECT 2000 + number, [if(number = 17, 0.1, 10 + number)], 0, 1000 + number FROM numbers(24);
CREATE DICTIONARY tie_dict (cid UInt64, vec Array(Float32), level UInt8, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'tie_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT groupUniqArray(assignCentroid([toFloat32(number * 0)], 'tie_dict')), count() FROM numbers(7);
DROP DICTIONARY tie_dict;
DROP TABLE tie_src;

SELECT '-- errors';
DROP DICTIONARY IF EXISTS err_dict;
DROP TABLE IF EXISTS err_src;
CREATE TABLE err_src (cid UInt64, vec Array(Float32), level UInt64, parent UInt64) ENGINE = MergeTree ORDER BY cid;
CREATE DICTIONARY err_dict (cid UInt64, vec Array(Float32), level UInt64, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'err_src')) LAYOUT(HASHED()) LIFETIME(0);
-- Only levels 0 and 1 exist.
INSERT INTO err_src VALUES (1, [0], 1, 0), (2, [0], 0, 1), (3, [0], 2, 0);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
-- A leaf whose parent is no centroid at all.
TRUNCATE TABLE err_src;
INSERT INTO err_src VALUES (1, [0], 1, 0), (2, [0], 0, 7);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
-- A leaf whose parent is another leaf.
TRUNCATE TABLE err_src;
INSERT INTO err_src VALUES (1, [0], 1, 0), (2, [0], 0, 1), (3, [0], 0, 2);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
-- No leaves.
TRUNCATE TABLE err_src;
INSERT INTO err_src VALUES (1, [0], 1, 0), (2, [5], 1, 0);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
-- Coarse and leaf centroids of different dimensions.
TRUNCATE TABLE err_src;
INSERT INTO err_src VALUES (1, [0, 0], 1, 0), (2, [0], 0, 1);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
-- A non-finite coarse centroid.
TRUNCATE TABLE err_src;
INSERT INTO err_src VALUES (1, [nan], 1, 0), (2, [0], 0, 1);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError INCORRECT_DATA }
-- The same rows are accepted once valid.
TRUNCATE TABLE err_src;
INSERT INTO err_src VALUES (1, [0], 1, 0), (2, [0], 0, 1);
SYSTEM RELOAD DICTIONARY err_dict;
SELECT assignCentroid([0]::Array(Float32), 'err_dict');
DROP DICTIONARY err_dict;

-- Exactly one of `level` and `parent`.
CREATE DICTIONARY err_dict (cid UInt64, vec Array(Float32), level UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'err_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY err_dict;
CREATE DICTIONARY err_dict (cid UInt64, vec Array(Float32), parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'err_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY err_dict;
DROP TABLE err_src;

-- `level` and `parent` must be unsigned integers, like `cid`.
DROP TABLE IF EXISTS err_level_src;
DROP TABLE IF EXISTS err_parent_src;
CREATE TABLE err_level_src (cid UInt64, vec Array(Float32), level Int32, parent UInt64) ENGINE = MergeTree ORDER BY cid;
CREATE TABLE err_parent_src (cid UInt64, vec Array(Float32), level UInt8, parent String) ENGINE = MergeTree ORDER BY cid;
INSERT INTO err_level_src VALUES (1, [0], 1, 0), (2, [0], 0, 1);
INSERT INTO err_parent_src VALUES (1, [0], 1, '0'), (2, [0], 0, '1');
CREATE DICTIONARY err_dict (cid UInt64, vec Array(Float32), level Int32, parent UInt64)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'err_level_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP DICTIONARY err_dict;
CREATE DICTIONARY err_dict (cid UInt64, vec Array(Float32), level UInt8, parent String)
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'err_parent_src')) LAYOUT(HASHED()) LIFETIME(0);
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP DICTIONARY err_dict;
DROP TABLE err_level_src;
DROP TABLE err_parent_src;

-- Two coarse centroids with the same cid make `parent` ambiguous. Only a complex key can hold them.
DROP TABLE IF EXISTS err_dup_src;
CREATE TABLE err_dup_src (k UInt64, cid UInt64, vec Array(Float32), level UInt8, parent UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO err_dup_src VALUES (1, 1, [0], 1, 0), (2, 1, [5], 1, 0), (3, 2, [0], 0, 1);
CREATE DICTIONARY err_dict (k UInt64, cid UInt64, vec Array(Float32), level UInt8, parent UInt64)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'err_dup_src')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
SELECT assignCentroid([0]::Array(Float32), 'err_dict'); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY err_dict;
DROP TABLE err_dup_src;

DROP DICTIONARY h_dict;
DROP TABLE expected;
DROP TABLE probes;
DROP TABLE h_src;
DROP TABLE leaf_src;
DROP TABLE coarse_src;
