-- Tests the two-level form `assignCentroid(vec, coarse_dict_name, fine_dict_name)`, where a small set of
-- coarse centroids partitions the fine ones and a vector is only compared against the fine centroids under
-- the coarse centroids nearest to it.

DROP DICTIONARY IF EXISTS two_level_coarse_dict;
DROP DICTIONARY IF EXISTS two_level_fine_dict;
DROP TABLE IF EXISTS two_level_coarse;
DROP TABLE IF EXISTS two_level_fine;

-- 64 well-separated coarse centroids: coarse `s` is 100 * e_s, i.e. far out along its own axis.
CREATE TABLE two_level_coarse (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO two_level_coarse
SELECT number, arrayMap(j -> toFloat32(if(j = number, 100, 0)), range(64))::Array(Float32) FROM numbers(64);

-- 40 fine centroids around each coarse one, still far closer to their own coarse centroid than to any
-- other, so the partition the ids describe is also the partition the geometry describes.
CREATE TABLE two_level_fine (cid UInt64, super_id UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO two_level_fine
SELECT number, intDiv(number, 40) AS super,
       arrayMap(j -> toFloat32(if(j = super, 100, 0) + (sipHash64(number, j) % 200) / 100.), range(64))::Array(Float32)
FROM numbers(2560);

CREATE DICTIONARY two_level_coarse_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'two_level_coarse')) LAYOUT(FLAT(MAX_ARRAY_SIZE 10000)) LIFETIME(0);
CREATE DICTIONARY two_level_fine_dict (cid UInt64, super_id UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'two_level_fine')) LAYOUT(FLAT(MAX_ARRAY_SIZE 10000)) LIFETIME(0);

SELECT '-- a probe copied from a fine centroid comes back with it';
SELECT count(), countIf(assignCentroid(vec, 'two_level_coarse_dict', 'two_level_fine_dict') = cid) FROM two_level_fine;

SELECT '-- and it agrees with a flat scan of the same fine centroids';
SELECT countIf(assignCentroid(vec, 'two_level_coarse_dict', 'two_level_fine_dict') = assignCentroid(vec, 'two_level_fine_dict'))
FROM two_level_fine;

SELECT '-- a perturbed probe keeps the same answer';
SELECT count(), countIf(assignCentroid(arrayMap(x -> x + 0.001, vec), 'two_level_coarse_dict', 'two_level_fine_dict') = cid)
FROM two_level_fine WHERE cid % 7 = 0;

SELECT '-- errors';
-- a fine centroid whose super_id names no coarse centroid
INSERT INTO two_level_fine VALUES (9999, 12345, arrayMap(j -> toFloat32(0), range(64)));
SYSTEM RELOAD DICTIONARY two_level_fine_dict;
SELECT assignCentroid(vec, 'two_level_coarse_dict', 'two_level_fine_dict') FROM two_level_fine LIMIT 1; -- { serverError BAD_ARGUMENTS }
-- inline centroids are not accepted by the two-level form
SELECT assignCentroid([1.0, 2.0]::Array(Float32), [[0.0, 0.0]]::Array(Array(Float32)), 'two_level_fine_dict'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP DICTIONARY two_level_fine_dict;
DROP DICTIONARY two_level_coarse_dict;
DROP TABLE two_level_fine;
DROP TABLE two_level_coarse;
