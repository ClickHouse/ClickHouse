DROP TABLE IF EXISTS users;

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

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

SELECT
    name,
    uid
FROM users
ORDER BY age ASC
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE users;
