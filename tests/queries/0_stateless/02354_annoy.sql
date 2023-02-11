-- Tags: no-fasttest, no-ubsan, no-cpu-aarch64, no-backward-compatibility-check

SET allow_experimental_annoy_index = 1;

DROP TABLE IF EXISTS 02354_annoy;

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5;

INSERT INTO 02354_annoy VALUES (1, [0.0, 0.0, 10.0]), (2, [0.0, 0.0, 10.5]), (3, [0.0, 0.0, 9.5]), (4, [0.0, 0.0, 9.7]), (5, [0.0, 0.0, 10.2]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

SELECT *
FROM 02354_annoy
WHERE L2Distance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 5;

SELECT *
FROM 02354_annoy
ORDER BY L2Distance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;

SET param_02354_target_vector='[0.0, 0.0, 10.0]';

SELECT *
FROM 02354_annoy
WHERE L2Distance(embedding, {02354_target_vector: Array(Float32)}) < 1.0
LIMIT 5;

SELECT *
FROM 02354_annoy
ORDER BY L2Distance(embedding, {02354_target_vector: Array(Float32)})
LIMIT 3;

SELECT *
FROM 02354_annoy
ORDER BY L2Distance(embedding, [0.0, 0.0])
LIMIT 3; -- { serverError 80 }

DROP TABLE IF EXISTS 02354_annoy;

-- ------------------------------------
-- Check that weird base columns are rejected

-- Index spans >1 column

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index (embedding, id) TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5; -- {serverError 7 }

-- Index must be created on Array(Float32) or Tuple(Float32)

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Float32,
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5; -- {serverError 44 }


CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Float64),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5; -- {serverError 44 }

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Tuple(Float32, Float64),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5; -- {serverError 44 }

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(LowCardinality(Float32)),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5; -- {serverError 44 }

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Nullable(Float32)),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5; -- {serverError 44 }
