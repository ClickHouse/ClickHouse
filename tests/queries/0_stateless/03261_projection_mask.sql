-- compact
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select age, count() group by age),
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

-- wide
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select age, count() group by age),
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
