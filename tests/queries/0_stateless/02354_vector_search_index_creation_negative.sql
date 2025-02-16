-- Tags: no-fasttest, no-ubsan, no-cpu-aarch64, no-ordinary-database, no-asan

-- Tests vector search in ClickHouse, i.e. Annoy and Usearch indexes. Both index types share similarities in implementation and usage,
-- therefore they are tested in a single file.

-- This file tests that various conditions are checked during creation of vector search indexes.

SET allow_experimental_annoy_index = 1;
SET allow_experimental_usearch_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'At most two index arguments';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy('too', 'many', 'arguments')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch('too', 'many', 'args')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '1st argument (distance function) must be String';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy(3)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch(3)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'Rejects unsupported distance functions';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy('invalidDistance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch('invalidDistance')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

SELECT '2nd argument (Annoy: number of trees, USearch: scalar kind) must be UInt64 (Annoy) / String (Usearch)';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy('L2Distance', 'not an UInt64')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch(3)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'Rejects unsupported scalar kinds (only Usearch)';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch('L2Distance', 'invalidKind')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

SELECT 'Must be created on single column';
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx (vec, id) TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx (vec, id) TYPE usearch()) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

SELECT 'Must be created on Array(Float32) or Tuple(Float32, Float, ...) columns';

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE tab(id Int32, vec Float32, INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Float32, INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE tab(id Int32, vec Array(Float64), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Array(Float64), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE tab(id Int32, vec Tuple(Float64), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Tuple(Float64), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE tab(id Int32, vec LowCardinality(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec LowCardinality(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE tab(id Int32, vec Nullable(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, vec Nullable(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

SELECT 'Rejects INSERTs of Arrays with different sizes';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE annoy()) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }
DROP TABLE tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE usearch()) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab values (0, [2.2, 2.3]) (1, [3.1, 3.2, 3.3]); -- { serverError INCORRECT_DATA }
DROP TABLE tab;
