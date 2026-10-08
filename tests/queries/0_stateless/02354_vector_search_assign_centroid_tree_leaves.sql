-- assignCentroid assigns only to leaves (level 0) when the centroids form a tree, not to centroids of upper levels.

CREATE TABLE centroids (level UInt8, cid UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY (level, cid);
INSERT INTO centroids VALUES (0, 1, [0, 0]), (0, 2, [10, 10]), (1, 100, [5, 5]);

CREATE DICTIONARY centroids_dict (level UInt8, cid UInt32, vec Array(Float32))
PRIMARY KEY level, cid
SOURCE(CLICKHOUSE(TABLE 'centroids' DB currentDatabase()))
LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

-- [4, 4] is nearest to the level 1 centroid 100, and among the leaves nearest to 1.
SELECT assignCentroid([toFloat32(4), 4], 'centroids_dict');
SELECT assignCentroid([toFloat32(6), 6], 'centroids_dict');
