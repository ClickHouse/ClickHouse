-- Tags: distributed

-- INTENTIONAL: in test_marker_suite_side and elsewhere, b0 ALIAS x / b1 ALIAS x
-- (and other paired aliases) reference identical underlying expressions. This shape
-- detects column-COLLAPSE in transport, NOT column-SWAP. Column-swap regressions are
-- covered by 04280_distributed_alias_column_order. Do not "fix" by giving the aliases
-- distinct values -- that would lose collapse coverage.

DROP TABLE IF EXISTS test_marker_suite_main_dist;
DROP TABLE IF EXISTS test_marker_suite_side_dist;
DROP TABLE IF EXISTS test_marker_suite_main;
DROP TABLE IF EXISTS test_marker_suite_side;
DROP TABLE IF EXISTS test_wrapper_alias_a_dist;
DROP TABLE IF EXISTS test_wrapper_alias_b_dist;
DROP TABLE IF EXISTS test_wrapper_alias_a_local;
DROP TABLE IF EXISTS test_wrapper_alias_b_local;
DROP TABLE IF EXISTS test_wrapper_const_alias_a_dist;
DROP TABLE IF EXISTS test_wrapper_const_alias_b_dist;
DROP TABLE IF EXISTS test_wrapper_const_alias_a_local;
DROP TABLE IF EXISTS test_wrapper_const_alias_b_local;

CREATE TABLE test_marker_suite_main
(
    id UInt64
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_marker_suite_main VALUES (1), (2);

CREATE TABLE test_marker_suite_side
(
    id UInt64,
    x UInt64,
    b0 UInt64 ALIAS x,
    b1 UInt64 ALIAS x
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_marker_suite_side VALUES (1, 10), (2, 20);

CREATE TABLE test_marker_suite_main_dist AS test_marker_suite_main
ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_marker_suite_main, rand());

CREATE TABLE test_marker_suite_side_dist AS test_marker_suite_side
ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_marker_suite_side, rand());

SELECT 'case1_global_in_unnamed_identical_derived_subqueries';
SELECT id
FROM test_marker_suite_main_dist
WHERE id GLOBAL IN
(
    SELECT left_id
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM test_marker_suite_side_dist)
        INNER JOIN
        (SELECT id AS right_id, b0 AS right_b0 FROM test_marker_suite_side_dist)
        ON left_id < right_id
    WHERE left_b0 + right_b0 = 30
    SETTINGS joined_subquery_requires_alias = 0
)
ORDER BY id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1;

SELECT 'case2_global_join_unnamed_identical_derived_subqueries';
SELECT m.id, j.left_b0, j.right_b0
FROM test_marker_suite_main_dist AS m
GLOBAL INNER JOIN
(
    SELECT left_id AS id, left_b0, right_b0
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM test_marker_suite_side_dist)
        INNER JOIN
        (SELECT id AS right_id, b0 AS right_b0 FROM test_marker_suite_side_dist)
        ON left_id < right_id
    WHERE left_b0 + right_b0 = 30
    SETTINGS joined_subquery_requires_alias = 0
) AS j
ON m.id = j.id
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1
FORMAT TSVWithNames;

SELECT 'case3_global_join_unnamed_identical_derived_subqueries_serialize_query_plan';
SELECT m.id, j.left_b0, j.right_b0
FROM test_marker_suite_main_dist AS m
GLOBAL INNER JOIN
(
    SELECT left_id AS id, left_b0, right_b0
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM test_marker_suite_side_dist)
        INNER JOIN
        (SELECT id AS right_id, b0 AS right_b0 FROM test_marker_suite_side_dist)
        ON left_id < right_id
    WHERE left_b0 + right_b0 = 30
    SETTINGS joined_subquery_requires_alias = 0
) AS j
ON m.id = j.id
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, serialize_query_plan = 1
FORMAT TSVWithNames;

SELECT 'case4_global_join_unnamed_remote_over_distributed_subqueries';
SELECT m.id, j.left_b0, j.right_b0
FROM test_marker_suite_main_dist AS m
GLOBAL INNER JOIN
(
    SELECT left_id AS id, left_b0, right_b0
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM remote('127.0.0.2', currentDatabase(), test_marker_suite_side_dist))
        INNER JOIN
        (SELECT id AS right_id, b0 AS right_b0 FROM remote('127.0.0.2', currentDatabase(), test_marker_suite_side_dist))
        ON left_id < right_id
    WHERE left_b0 + right_b0 = 30
    SETTINGS joined_subquery_requires_alias = 0
) AS j
ON m.id = j.id
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1
FORMAT TSVWithNames;

SELECT 'case5_global_join_unnamed_identical_dual_alias_columns';
SELECT m.id, j.left_b0, j.right_b1
FROM test_marker_suite_main_dist AS m
GLOBAL INNER JOIN
(
    SELECT left_id AS id, left_b0, right_b1
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM test_marker_suite_side_dist)
        INNER JOIN
        (SELECT id AS right_id, b1 AS right_b1 FROM test_marker_suite_side_dist)
        ON left_id < right_id
    WHERE left_b0 + right_b1 = 30
    SETTINGS joined_subquery_requires_alias = 0
) AS j
ON m.id = j.id
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1
FORMAT TSVWithNames;

SELECT 'case6_local_join_unnamed_identical_derived_subqueries';
SELECT m.id, j.left_b0, j.right_b0
FROM test_marker_suite_main_dist AS m
INNER JOIN
(
    SELECT left_id AS id, left_b0, right_b0
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM test_marker_suite_side_dist)
        INNER JOIN
        (SELECT id AS right_id, b0 AS right_b0 FROM test_marker_suite_side_dist)
        ON left_id < right_id
    WHERE left_b0 + right_b0 = 30
    SETTINGS joined_subquery_requires_alias = 0
) AS j
ON m.id = j.id
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, distributed_product_mode = 'local'
FORMAT TSVWithNames;

SELECT 'case7_local_join_unnamed_identical_derived_subqueries_serialize_query_plan';
SELECT m.id, j.left_b0, j.right_b0
FROM test_marker_suite_main_dist AS m
INNER JOIN
(
    SELECT left_id AS id, left_b0, right_b0
    FROM
        (SELECT id AS left_id, b0 AS left_b0 FROM test_marker_suite_side_dist)
        INNER JOIN
        (SELECT id AS right_id, b0 AS right_b0 FROM test_marker_suite_side_dist)
        ON left_id < right_id
    WHERE left_b0 + right_b0 = 30
    SETTINGS joined_subquery_requires_alias = 0
) AS j
ON m.id = j.id
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, distributed_product_mode = 'local', serialize_query_plan = 1
FORMAT TSVWithNames;

SELECT 'case8_global_join_direct_distributed_serialize_query_plan';
SELECT m.id, b0, b1
FROM test_marker_suite_main_dist AS m
GLOBAL INNER JOIN test_marker_suite_side_dist USING (id)
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, asterisk_include_alias_columns = 1, serialize_query_plan = 1
FORMAT TSVWithNames;

SELECT 'case9_global_join_direct_remote_over_distributed_serialize_query_plan';
SELECT m.id, b0, b1
FROM test_marker_suite_main_dist AS m
GLOBAL INNER JOIN remote('127.0.0.2', currentDatabase(), test_marker_suite_side_dist) USING (id)
ORDER BY m.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, asterisk_include_alias_columns = 1, joined_subquery_requires_alias = 0, serialize_query_plan = 1
FORMAT TSVWithNames;

DROP TABLE test_marker_suite_main_dist;
DROP TABLE test_marker_suite_side_dist;
DROP TABLE test_marker_suite_main;
DROP TABLE test_marker_suite_side;

CREATE TABLE test_wrapper_alias_a_local
(
    id UInt64,
    x UInt64
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_wrapper_alias_b_local
(
    id UInt64,
    x UInt64
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_wrapper_alias_a_local VALUES (1, 1), (2, 20);
INSERT INTO test_wrapper_alias_b_local VALUES (1, 1), (2, 20);

CREATE TABLE test_wrapper_alias_a_dist
(
    id UInt64,
    x UInt64,
    foo UInt64 ALIAS x
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_wrapper_alias_a_local, rand());

CREATE TABLE test_wrapper_alias_b_dist
(
    id UInt64,
    x UInt64,
    foo UInt64 ALIAS x
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_wrapper_alias_b_local, rand());

SELECT 'case10_wrapper_alias_subquery_serialize_query_plan';
SELECT a.id, j.left_foo, j.right_foo
FROM test_wrapper_alias_a_dist AS a
GLOBAL INNER JOIN
(
    SELECT l.id, l.foo AS left_foo, r.foo AS right_foo
    FROM test_wrapper_alias_a_dist AS l
    INNER JOIN test_wrapper_alias_b_dist AS r ON l.id < r.id
    WHERE l.foo + r.foo = 21
) AS j
ON a.id = j.id
ORDER BY a.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, serialize_query_plan = 1
FORMAT TSVWithNames;

DROP TABLE test_wrapper_alias_a_dist;
DROP TABLE test_wrapper_alias_b_dist;
DROP TABLE test_wrapper_alias_a_local;
DROP TABLE test_wrapper_alias_b_local;

CREATE TABLE test_wrapper_const_alias_a_local
(
    id UInt64
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_wrapper_const_alias_b_local
(
    id UInt64
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_wrapper_const_alias_a_local VALUES (1), (2);
INSERT INTO test_wrapper_const_alias_b_local VALUES (1), (2);

CREATE TABLE test_wrapper_const_alias_a_dist
(
    id UInt64,
    foo String ALIAS 'foo'
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_wrapper_const_alias_a_local, rand());

CREATE TABLE test_wrapper_const_alias_b_dist
(
    id UInt64,
    foo String ALIAS 'foo'
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_wrapper_const_alias_b_local, rand());

SELECT 'case11_wrapper_constant_alias_subquery_serialize_query_plan';
SELECT a.id, j.left_foo, j.right_foo
FROM test_wrapper_const_alias_a_dist AS a
GLOBAL INNER JOIN
(
    SELECT l.id, l.foo AS left_foo, r.foo AS right_foo
    FROM test_wrapper_const_alias_a_dist AS l
    INNER JOIN test_wrapper_const_alias_b_dist AS r ON l.id = r.id
    WHERE l.foo = 'foo' AND r.foo = 'foo'
) AS j
ON a.id = j.id
ORDER BY a.id
SETTINGS enable_alias_marker = 1, enable_analyzer = 1, serialize_query_plan = 1
FORMAT TSVWithNames;

DROP TABLE test_wrapper_const_alias_a_dist;
DROP TABLE test_wrapper_const_alias_b_dist;
DROP TABLE test_wrapper_const_alias_a_local;
DROP TABLE test_wrapper_const_alias_b_local;
