-- Tests the hidden centroids table and dictionary created with an IVF vector similarity index

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_renamed;
DROP TABLE IF EXISTS samples;

CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT [toFloat32(number), toFloat32(number % 10)] FROM numbers(100);

SET allow_experimental_vector_ivf_index = 1;

CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'cosineDistance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id);

SELECT 'Hidden objects';
SELECT replaceOne(name, (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'tab'), '<uuid>') AS name, engine, partition_key, sorting_key
FROM system.tables
WHERE database = currentDatabase() AND startsWith(name, '_ivf_')
ORDER BY name;

SELECT 'Centroids table';
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND startsWith(table, '_ivf_quantizer_')
ORDER BY position;

SELECT 'Centroids dictionary';
SELECT key.names, key.types, attribute.names, attribute.types FROM system.dictionaries
WHERE database = currentDatabase() AND startsWith(name, '_ivf_centroids_dict_');

SELECT 'Objects keep their names after RENAME';
RENAME TABLE tab TO tab_renamed;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND startsWith(name, '_ivf_');

SELECT 'Objects are dropped with the table';
DROP TABLE tab_renamed;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND startsWith(name, '_ivf_');

SELECT 'Tables without an IVF index have no hidden objects';
CREATE TABLE tab (id UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND startsWith(name, '_ivf_');
DROP TABLE tab;

DROP TABLE samples;
