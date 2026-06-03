
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

-- tuple subcolumn in projection ORDER BY (PR #99043)
-- _row_exists must be added to source_block separately from projection columns,
-- and addSubcolumnsFromSortingKeyAndSkipIndicesExpression must materialize tup.a.
DROP TABLE IF EXISTS users_tuple;

CREATE TABLE users_tuple (
    uid Int16,
    tup Tuple(a UInt32, b String),
    projection p (select tup order by tup.a)
) ENGINE = MergeTree ORDER BY uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO users_tuple VALUES (1, (10, 'x')), (2, (20, 'y')), (3, (30, 'z'));

DELETE FROM users_tuple WHERE uid = 1;

SELECT tup FROM users_tuple WHERE tup.a = 20
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE users_tuple;

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
