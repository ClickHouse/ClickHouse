-- Tests the declaration of IVF vector similarity indexes:
--     vector_similarity('ivf', 'distance_function', dimensions, samples = 'samples_table', clustering = clustering_function(k[, ...]))

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS samples;
DROP TABLE IF EXISTS samples_other_column;
DROP VIEW IF EXISTS samples_view;

CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT [toFloat32(number), toFloat32(number % 10)] FROM numbers(100);

SELECT 'Experimental setting';
SET allow_experimental_vector_ivf_index = 0;
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError SUPPORT_IS_DISABLED }
SET allow_experimental_vector_ivf_index = 1;

SELECT 'Invalid positional arguments';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf')) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance')) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 1, 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'dotProduct', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 0, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 'two', samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }

SELECT 'Invalid named arguments';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples')) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4), nprobe = 8)) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = '', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 1, clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = 'hierarchicalKMeans')) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans())) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(0))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(vec))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4), cluster_statistics = 2)) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4), cluster_statistics = 'yes')) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4), cluster_statistics = 1, cluster_statistics = 0)) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4), target_cluster_size = 0)) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4), target_cluster_size = 'large')) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_DATA }

CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX `i.dx` (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_QUERY }

SELECT 'Invalid columns';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError INCORRECT_NUMBER_OF_COLUMNS }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (cid, vec) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(String), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab (id UInt64, cid UInt32, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError ILLEGAL_COLUMN }

SELECT 'At most one IVF index per table';
CREATE TABLE tab (id UInt64, cid UInt64, cid2 UInt64, vec Array(Float32), vec2 Array(Float32),
    INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4)),
    INDEX idx2 (vec2, cid2) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4)))
ENGINE = MergeTree ORDER BY (cid, cid2, id); -- { serverError BAD_ARGUMENTS }

SELECT 'IVF index can not be added to an existing table';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY (cid, id);
ALTER TABLE tab ADD INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4)); -- { serverError NOT_IMPLEMENTED }
DROP TABLE tab;

SELECT 'No CREATE OR REPLACE, REPLACE or CREATE ... AS SELECT';
CREATE OR REPLACE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError NOT_IMPLEMENTED }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id) AS SELECT 1, 1, [1., 2.]; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY (cid, id);
REPLACE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError NOT_IMPLEMENTED }
DROP TABLE tab;

SELECT 'No CREATE TABLE ... AS or CLONE AS';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id);
CREATE TABLE tab_copy AS tab; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE tab_copy CLONE AS tab; -- { serverError NOT_IMPLEMENTED }
DROP TABLE tab;

SELECT 'Cluster id column must be in the primary key';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (intHash32(cid), id); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (tenant UInt32, id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (tenant, cid, id);
DROP TABLE tab;

CREATE TABLE tab (tenant UInt32, id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree PRIMARY KEY tenant ORDER BY (tenant, cid, id); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (tenant UInt32, id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree PRIMARY KEY (tenant, cid) ORDER BY (tenant, cid, id);
DROP TABLE tab;

SELECT 'Samples table must exist';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'no_such_samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError UNKNOWN_TABLE }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = no_such_database.samples, clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError UNKNOWN_TABLE }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = samples, clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id);
DROP TABLE tab;

SELECT 'Samples table must have the vector column';
CREATE TABLE samples_other_column (embedding Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples_other_column', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT 'Clustering function must accept the parameters and return the centroids';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = noSuchClusteringFunction(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4, 1))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4, 16, 20, 100, 42, 1, 7))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = uniqUpTo(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError BAD_ARGUMENTS }

SELECT 'Samples table must have at least k rows';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(101, 16, 20, 1000))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError BAD_ARGUMENTS }
CREATE VIEW samples_view AS SELECT vec FROM samples;
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples_view', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(Float32), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'cosineDistance', 2, samples = 'samples', clustering = hierarchicalKMeans(100, 16, 20, 1000, 42, 1), cluster_statistics = 1)) ENGINE = MergeTree ORDER BY (cid, id);
DROP TABLE tab;

SELECT 'Valid declaration';
CREATE TABLE tab (id UInt64, cid UInt64, vec Array(BFloat16), INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))) ENGINE = MergeTree ORDER BY (cid, id);
SELECT name, type, type_full, granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

SELECT 'Vectors must have the declared dimensions';
INSERT INTO tab VALUES (1, 1, [1, 2, 3]); -- { serverError INCORRECT_DATA }

SELECT 'Insert and merge';
INSERT INTO tab SELECT number, number % 7, [toBFloat16(number), 0] FROM numbers(10000);
INSERT INTO tab SELECT number, number % 11, [toBFloat16(number), 1] FROM numbers(10000);
OPTIMIZE TABLE tab FINAL;
SELECT count(), uniqExact(cid) FROM tab;
SELECT data_compressed_bytes > 0 FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';
SELECT cid, count() FROM tab WHERE cid IN (3, 9) GROUP BY cid ORDER BY cid;

SELECT 'Table stays usable after the samples table is dropped';
DROP VIEW samples_view;
DROP TABLE samples;
DETACH TABLE tab;
ATTACH TABLE tab;
ALTER TABLE tab MODIFY SETTING max_parts_to_merge_at_once = 10;

SELECT 'IVF index can not be dropped';
ALTER TABLE tab DROP INDEX idx; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE tab CLEAR INDEX idx;
SELECT count() FROM tab;

DROP TABLE tab;
DROP TABLE samples_other_column;
