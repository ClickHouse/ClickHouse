-- Tags: no-fasttest, no-ubsan, no-cpu-aarch64, no-upgrade-check

SET allow_experimental_annoy_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Negative tests';

-- must have at most 2 arguments
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy('too', 'many', 'arguments')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- first argument must be UInt64
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy(3)) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- 2nd argument must be String
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index embedding TYPE annoy('L2Distance', 'not an UInt64')) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

-- must be created on single column
CREATE TABLE tab(id Int32, embedding Array(Float32), INDEX annoy_index (embedding, id) TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

-- must be created on Array/Tuple(Float32) columns
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE tab(id Int32, embedding Float32, INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, embedding Array(Float64), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, embedding LowCardinality(Float32), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE tab(id Int32, embedding Nullable(Float32), INDEX annoy_index embedding TYPE annoy()) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
