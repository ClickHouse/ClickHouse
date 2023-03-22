#!/usr/bin/env bash
# Tags: no-fasttest, no-ubsan, no-cpu-aarch64, no-backward-compatibility-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Check that index works correctly for L2Distance and with client parameters
$CLICKHOUSE_CLIENT -nm --allow_experimental_annoy_index=1 -q "
DROP TABLE IF EXISTS 02354_annoy_l2;

CREATE TABLE 02354_annoy_l2
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index embedding TYPE annoy() GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi';

INSERT INTO 02354_annoy_l2 VALUES (1, [0.0, 0.0, 10.0]), (2, [0.0, 0.0, 10.5]), (3, [0.0, 0.0, 9.5]), (4, [0.0, 0.0, 9.7]), (5, [0.0, 0.0, 10.2]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

SELECT *
FROM 02354_annoy_l2
WHERE L2Distance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 5;

SELECT *
FROM 02354_annoy_l2
ORDER BY L2Distance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;

SET param_02354_target_vector='[0.0, 0.0, 10.0]';

SELECT *
FROM 02354_annoy_l2
WHERE L2Distance(embedding, {02354_target_vector: Array(Float32)}) < 1.0
LIMIT 5;

SELECT *
FROM 02354_annoy_l2
ORDER BY L2Distance(embedding, {02354_target_vector: Array(Float32)})
LIMIT 3;

SELECT *
FROM 02354_annoy_l2
ORDER BY L2Distance(embedding, [0.0, 0.0])
LIMIT 3; -- { serverError 80 }


DROP TABLE IF EXISTS 02354_annoy_l2;
"

# Check that indexes are used
$CLICKHOUSE_CLIENT -nm --allow_experimental_annoy_index=1 -q "
DROP TABLE IF EXISTS 02354_annoy_l2;

CREATE TABLE 02354_annoy_l2
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index embedding TYPE annoy() GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi';

INSERT INTO 02354_annoy_l2 VALUES (1, [0.0, 0.0, 10.0]), (2, [0.0, 0.0, 10.5]), (3, [0.0, 0.0, 9.5]), (4, [0.0, 0.0, 9.7]), (5, [0.0, 0.0, 10.2]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

EXPLAIN indexes=1
SELECT *
FROM 02354_annoy_l2
WHERE L2Distance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 5;

EXPLAIN indexes=1
SELECT *
FROM 02354_annoy_l2
ORDER BY L2Distance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;
DROP TABLE IF EXISTS 02354_annoy_l2;
" | grep "annoy_index"


# # Check that index works correctly for cosineDistance
$CLICKHOUSE_CLIENT -nm --allow_experimental_annoy_index=1 -q "
DROP TABLE IF EXISTS 02354_annoy_cosine;

CREATE TABLE 02354_annoy_cosine
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index embedding TYPE annoy(100, 'cosineDistance') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi';

INSERT INTO 02354_annoy_cosine VALUES (1, [0.0, 0.0, 10.0]), (2, [0.2, 0.0, 10.0]), (3, [-0.3, 0.0, 10.0]), (4, [0.5, 0.0, 10.1]), (5, [0.8, 0.0, 10.0]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

SELECT *
FROM 02354_annoy_cosine
WHERE cosineDistance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 3;

SELECT *
FROM 02354_annoy_cosine
ORDER BY cosineDistance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;

DROP TABLE IF EXISTS 02354_annoy_cosine;
"

# # Check that indexes are used
$CLICKHOUSE_CLIENT -nm --allow_experimental_annoy_index=1 -q "
DROP TABLE IF EXISTS 02354_annoy_cosine;

CREATE TABLE 02354_annoy_cosine
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index embedding TYPE annoy(100, 'cosineDistance') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi';

INSERT INTO 02354_annoy_cosine VALUES (1, [0.0, 0.0, 10.0]), (2, [0.2, 0.0, 10.0]), (3, [-0.3, 0.0, 10.0]), (4, [0.5, 0.0, 10.1]), (5, [0.8, 0.0, 10.0]), (6, [10.0, 0.0, 0.0]), (7, [9.5, 0.0, 0.0]), (8, [9.7, 0.0, 0.0]), (9, [10.2, 0.0, 0.0]), (10, [10.5, 0.0, 0.0]), (11, [0.0, 10.0, 0.0]), (12, [0.0, 9.5, 0.0]), (13, [0.0, 9.7, 0.0]), (14, [0.0, 10.2, 0.0]), (15, [0.0, 10.5, 0.0]);

EXPLAIN indexes=1
SELECT *
FROM 02354_annoy_cosine
WHERE cosineDistance(embedding, [0.0, 0.0, 10.0]) < 1.0
LIMIT 3;

EXPLAIN indexes=1
SELECT *
FROM 02354_annoy_cosine
ORDER BY cosineDistance(embedding, [0.0, 0.0, 10.0])
LIMIT 3;
DROP TABLE IF EXISTS 02354_annoy_cosine;
" | grep "annoy_index"

# # Check that weird base columns are rejected
$CLICKHOUSE_CLIENT -nm --allow_experimental_annoy_index=1 -q "
DROP TABLE IF EXISTS 02354_annoy;

-- Index spans >1 column

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Float32),
    INDEX annoy_index (embedding, id) TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi'; -- {serverError 7 }

-- Index must be created on Array(Float32) or Tuple(Float32)

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Float32,
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi'; -- {serverError 44 }


CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Float64),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi'; -- {serverError 44 }

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Tuple(Float32, Float64),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi'; -- {serverError 44 }

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(LowCardinality(Float32)),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi'; -- {serverError 44 }

CREATE TABLE 02354_annoy
(
    id Int32,
    embedding Array(Nullable(Float32)),
    INDEX annoy_index embedding TYPE annoy(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=5, index_granularity_bytes = '10Mi'; -- {serverError 44 }"
