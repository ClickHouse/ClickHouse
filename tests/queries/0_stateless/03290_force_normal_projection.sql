CREATE TABLE users
(
    `uid` Int16,
    `name` String,
    `age` Int16,
    PROJECTION p1
    (
        SELECT 
            name,
            uid
        ORDER BY age
    )
)
ENGINE = MergeTree
ORDER BY uid;

INSERT INTO users VALUES (1231, 'John', 33), (1232, 'Mary', 34);

SELECT
    name,
    uid
FROM users
ORDER BY age ASC
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
