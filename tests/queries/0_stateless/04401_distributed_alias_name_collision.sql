-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/107990
-- A distributed JOIN where both tables define an ALIAS column with the SAME name, referenced in an
-- expression clause (WHERE / GROUP BY / ORDER BY / JOIN ON), used to fail with
-- MULTIPLE_EXPRESSIONS_FOR_ALIAS: inlining each ALIAS column re-applied the bare column name as an
-- alias, so two different expressions ended up sharing one alias in the same scope. The inlined
-- expression must keep its alias only as a top-level projection output, not inside expression clauses.

DROP TABLE IF EXISTS na_local;
DROP TABLE IF EXISTS nb_local;
DROP TABLE IF EXISTS na_dist;
DROP TABLE IF EXISTS nb_dist;

CREATE TABLE na_local (id UInt64, x UInt64, foo UInt64 ALIAS x) ENGINE = MergeTree ORDER BY id;
CREATE TABLE nb_local (id UInt64, x UInt64, foo UInt64 ALIAS x) ENGINE = MergeTree ORDER BY id;
CREATE TABLE na_dist AS na_local ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), na_local, rand());
CREATE TABLE nb_dist AS nb_local ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), nb_local, rand());

INSERT INTO na_local VALUES (1, 5), (2, 7);
INSERT INTO nb_local VALUES (1, 50), (2, 70);

SET enable_analyzer = 1, distributed_product_mode = 'local';

-- JOIN ON references the same-named alias column on both sides (exercises the join-tree branch).
SELECT 'join_on';
SELECT l.id FROM na_dist AS l INNER JOIN nb_dist AS r ON l.id = r.id AND l.foo > 0 AND r.foo > 0 ORDER BY l.id;

-- WHERE references the same-named alias column on both JOIN sides.
SELECT 'where';
SELECT l.id FROM na_dist AS l INNER JOIN nb_dist AS r ON l.id = r.id WHERE l.foo > 0 AND r.foo > 0 ORDER BY l.id;

-- GROUP BY on the same-named alias columns from both sides.
SELECT 'group_by';
SELECT l.foo AS lf, r.foo AS rf, count() AS c FROM na_dist AS l INNER JOIN nb_dist AS r ON l.id = r.id GROUP BY l.foo, r.foo ORDER BY lf, rf;

-- ORDER BY on the same-named alias columns from both sides.
SELECT 'order_by';
SELECT l.id FROM na_dist AS l INNER JOIN nb_dist AS r ON l.id = r.id ORDER BY l.foo, r.foo, l.id;

-- A projection expression combining the same-named alias columns from both sides.
SELECT 'projection_expr';
SELECT l.foo + r.foo AS s FROM na_dist AS l INNER JOIN nb_dist AS r ON l.id = r.id ORDER BY s;

-- GLOBAL JOIN wrapping a subquery that references the same-named alias columns (the original report).
SELECT 'global_join_subquery';
SELECT a.id, j.left_foo, j.right_foo
FROM na_dist AS a
GLOBAL INNER JOIN
(
    SELECT l.id, l.foo AS left_foo, r.foo AS right_foo
    FROM na_dist AS l
    INNER JOIN nb_dist AS r ON l.id = r.id
    WHERE l.foo + r.foo > 0
) AS j
ON a.id = j.id
ORDER BY a.id, j.left_foo, j.right_foo;

DROP TABLE na_dist;
DROP TABLE nb_dist;
DROP TABLE na_local;
DROP TABLE nb_local;
