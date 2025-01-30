-- compact
DROP TABLE IF EXISTS users_compact;

CREATE TABLE users_compact (
    uid Int16,
    name String,
    age Int16,
    projection p1 (SELECT age, count() GROUP BY age),
    projection p2 (SELECT age, avg(age) GROUP BY age),
    projection p3 (SELECT name, uid ORDER BY age),
    projection p4 (SELECT count() GROUP BY name)
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = 10485760;

INSERT INTO users_compact VALUES (1231, 'John', 33), (1232, 'Mary', 34);

DELETE FROM users_compact WHERE age = 34;

SELECT
    age,
    count()
FROM users_compact
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    age,
    avg(age)
FROM users_compact
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    name,
    uid
FROM users_compact
ORDER BY age ASC
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    count()
FROM users_compact
GROUP BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE users_compact;

-- wide
DROP TABLE IF EXISTS users_wide;

CREATE TABLE users_wide (
    uid Int16,
    name String,
    age Int16,
    projection p1 (SELECT age, count() GROUP BY age),
    projection p2 (SELECT age, avg(age) GROUP BY age),
    projection p3 (SELECT name, uid ORDER BY age),
    projection p4 (SELECT count() GROUP BY name)
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = 0;

INSERT INTO users_wide VALUES (1231, 'John', 33), (1232, 'Mary', 34);

DELETE FROM users_wide WHERE age = 34;

SELECT
    age,
    count()
FROM users_wide
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    age,
    avg(age)
FROM users_wide
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    name,
    uid
FROM users_wide
ORDER BY age ASC
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    count()
FROM users_wide
GROUP BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE users_wide;

-- test lightweight delete check condition (probably better put into lwd related)
-- as with ctx->updated_header.has(RowExistsColumn::name) is not enough
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `a` int,
    `b` int,
    INDEX idx b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test SELECT
    number,
    number
FROM numbers(10);

DELETE FROM test WHERE a = 5;

ALTER TABLE test
    (MATERIALIZE INDEX idx);

DROP TABLE test;