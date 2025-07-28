SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;

-- Disable table swaps during query planning
SET query_plan_join_swap_table = false;

CREATE TABLE test(
    i1 Int64,
    i2 Int64,
    i3 Int64,
    i4 Int64,
    i5 Int64,
    i6 Int64,
    i7 Int64,
    i8 Int64,
    i9 Int64,
    i10 Int64
)
ENGINE = MergeTree()
ORDER BY ();

INSERT INTO test VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

SET correlated_subqueries_substitute_equivalent_expressions = 0;

EXPLAIN actions = 1
SELECT 1 FROM test AS t1
WHERE EXISTS (
    SELECT * FROM test AS t2
    WHERE t1.i1 = t2.i2
);
