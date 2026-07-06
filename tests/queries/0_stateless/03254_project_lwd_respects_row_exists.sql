
-- compact test
DROP TABLE IF EXISTS users_compact;

CREATE TABLE users_compact (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select age, count() group by age),
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild',
min_bytes_for_wide_part = 10485760;

INSERT INTO users_compact VALUES (1231, 'John', 33), (1232, 'Mary', 34);

DELETE FROM users_compact WHERE uid = 1231;

SELECT
    age,
    count()
FROM users_compact
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

-- wide test
DROP TABLE IF EXISTS users_wide;

CREATE TABLE users_wide (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select age, count() group by age),
) ENGINE = MergeTree order by uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild',
min_bytes_for_wide_part = 0;

INSERT INTO users_wide VALUES (1231, 'John', 33), (1232, 'Mary', 34);

DELETE FROM users_wide WHERE uid = 1231;

SELECT
    age,
    count()
FROM users_wide
GROUP BY age
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;
