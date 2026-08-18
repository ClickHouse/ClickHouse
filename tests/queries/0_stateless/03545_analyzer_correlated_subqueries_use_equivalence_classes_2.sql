SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_substitute_equivalent_expressions=1;
SET query_plan_join_swap_table = false;
SET correlated_subqueries_default_join_kind = 'left';

CREATE TABLE a(c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree() ORDER BY ();
CREATE TABLE b(c1 Int64, c2 Int64, c3 Int64, c4 Int64) ENGINE = MergeTree() ORDER BY ();

INSERT INTO a VALUES (1, 1, 1, 1), (2, 1, 1, 2);
INSERT INTO b VALUES (1, 1, 1, 1), (1, 2, 2, 1);

-- {echoOn}
-- All columns in subquery condition belong to the same equivalence class
EXPLAIN
SELECT
    c1,
    (
        SELECT max(c3)
        FROM  a
        WHERE a.c1 = b.c2 AND b.c1 = b.c3 AND b.c1 = b.c2 AND b.c2 = b.c4
    )
FROM b;

-- Same query but with slightly different order of conditions in subquery
EXPLAIN
SELECT
    c1,
    (
        SELECT max(c3)
        FROM  a
        WHERE a.c1 = b.c2 AND b.c1 = b.c2 AND b.c1 = b.c3 AND b.c2 = b.c4
    )
FROM b;
