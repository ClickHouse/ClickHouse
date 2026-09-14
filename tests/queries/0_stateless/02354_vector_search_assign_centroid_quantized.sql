-- Tests the approximate path `assignCentroid` takes past 32768 centroids, where the centroids are RaBitQ
-- encoded and only a shortlist of them is scored exactly.

DROP TABLE IF EXISTS quantized_centroids;
DROP DICTIONARY IF EXISTS quantized_centroids_dict;

-- 40000 centroids, each a +-1 vector whose signs are the 64 bits of `sipHash64(cid)`. Distinct centroids are
-- then near-orthogonal, which is what a 1-bit code can tell apart, so a probe copied from a centroid has to
-- come back with that centroid however short the shortlist is.
CREATE TABLE quantized_centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO quantized_centroids
SELECT number, arrayMap(j -> toFloat32(if(bitTest(sipHash64(number), j), 1, -1)), range(64))::Array(Float32)
FROM numbers(40000);

CREATE DICTIONARY quantized_centroids_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'quantized_centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 100000)) LIFETIME(0);

SELECT '-- the 40000 centroids really are distinct, or the checks below would be vacuous';
SELECT uniqExact(vec) FROM quantized_centroids;

SELECT '-- above the threshold a probe still finds the centroid it was copied from';
SELECT count(), countIf(assignCentroid(vec, 'quantized_centroids_dict') = cid)
FROM quantized_centroids WHERE cid % 397 = 0;

SELECT '-- scaling and shifting a probe keeps the same nearest centroid, and the answer follows';
SELECT count(), countIf(assignCentroid(arrayMap(x -> x * 1.5 + 0.25, vec), 'quantized_centroids_dict') = cid)
FROM quantized_centroids WHERE cid % 397 = 0;

DROP DICTIONARY quantized_centroids_dict;
DROP TABLE quantized_centroids;

SELECT '-- a dimension that is not a multiple of 8 cannot be RaBitQ encoded, so it stays exact';
DROP TABLE IF EXISTS odd_centroids;
DROP DICTIONARY IF EXISTS odd_centroids_dict;
CREATE TABLE odd_centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
INSERT INTO odd_centroids
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 997) / 997, range(5))::Array(Float32)
FROM numbers(40000);
CREATE DICTIONARY odd_centroids_dict (cid UInt64, vec Array(Float32))
PRIMARY KEY cid SOURCE(CLICKHOUSE(TABLE 'odd_centroids')) LAYOUT(FLAT(MAX_ARRAY_SIZE 100000)) LIFETIME(0);

SELECT count(), countIf(assignCentroid(vec, 'odd_centroids_dict') = cid) FROM odd_centroids WHERE cid % 4001 = 0;

DROP DICTIONARY odd_centroids_dict;
DROP TABLE odd_centroids;
