-- compact
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (SELECT age, count() GROUP BY age),
    projection p2 (SELECT age, avg(age) GROUP BY age),
    projection p3 (SELECT name, uid ORDER BY age),
    projection p4 (SELECT count() GROUP BY name)
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = 10485760;

INSERT INTO users VALUES (1231, 'John', 33), (1232, 'Mary', 34);

DELETE FROM users WHERE age = 34;

SELECT
    age,
    count()
FROM users
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    age,
    avg(age)
FROM users
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    count()
FROM users
GROUP BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- wide
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (SELECT age, count() GROUP BY age),
    projection p2 (SELECT age, avg(age) GROUP BY age),
    projection p3 (SELECT name, uid ORDER BY age),
    projection p4 (SELECT count() GROUP BY name)
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = 0;

INSERT INTO users VALUES (1231, 'John', 33), (1232, 'Mary', 34);

DELETE FROM users WHERE age = 34;

SELECT
    age,
    count()
FROM users
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    age,
    avg(age)
FROM users
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT
    count()
FROM users
GROUP BY name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- test lightweight delete check condition (probably better put into lwd related)
-- as with ctx->updated_header.has(RowExistsColumn::name) is not enough
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
