-- Found by AST fuzzer. An expression over a `UNION ALL` of aggregate-state columns with the same
-- state representation but different functions (`quantileState` vs `quantilesState(0.9)`) wraps
-- the states into a `Tuple`. The per-branch headers then differ only by the aggregate function
-- nested inside the tuple, and the block structure checks (e.g. after the `liftUpUnion`
-- optimization and in `QueryPipeline`) compared such nested columns strictly by name, failing
-- debug and sanitizer builds with a logical error "Block structure mismatch".
SELECT tuple(s) FROM
(
    SELECT quantileState(number) AS s FROM numbers(7)
    UNION ALL
    SELECT quantilesState(0.9)(number) FROM numbers(5)
) FORMAT Null;

SELECT count() FROM
(
    SELECT tuple(s) FROM
    (
        SELECT quantileState(number) AS s FROM numbers(7)
        UNION ALL
        SELECT quantilesState(0.9)(number) FROM numbers(5)
    )
);

-- Constant aggregate-state columns. The constant-value comparison in the block structure check
-- must relax the aggregate-state leaves: comparing aggregate states as `Field` throws when the
-- aggregate function type names differ, even though the states are compatible by state
-- representation.
SELECT count() FROM
(
    SELECT arrayReduce('quantileState(0.5)', [1]) AS s
    UNION ALL
    SELECT arrayReduce('quantilesState(0.9)', [1]) AS s
);

-- The same, but the constant aggregate state is nested inside a `Tuple`.
SELECT count() FROM
(
    SELECT tuple(arrayReduce('quantileState(0.5)', [1])) AS s
    UNION ALL
    SELECT tuple(arrayReduce('quantilesState(0.9)', [1])) AS s
);

-- The queries below build a `Variant` from a set operation over unrelated types, which only the
-- analyzer does (the old one fails to find a common type for the branches).
SET enable_analyzer = 1;

-- The same, but the aggregate state is nested inside a `Variant`: a set operation over two
-- unrelated `Tuple` types builds a `Variant` of both, and the aggregate state lives inside one of
-- the alternatives. The `Variant` columns then differ only by the nested aggregate function, so the
-- block structure check must descend into `Variant` alternatives too.
SELECT count() IGNORE NULLS FROM
(
    (SELECT tuple(3, quantileState(number)) FROM numbers(7))
    EXCEPT ALL
    (SELECT tuple(quantilesState(0.9)(number), toInt128(2)) FROM numbers(5))
);

-- The alternatives inside a `Variant` column are stored in a local order that may differ from the
-- global (type) order and between the two sides of a `UNION`. The check must compare the
-- alternatives by global discriminator (the order the column name lists them in), not in the
-- storage order, otherwise same-typed `Variant` columns are reported as a structure mismatch.
SELECT count() FROM
(
    SELECT n, m FROM
    (
        (SELECT 2 AS n, map('z', 'a') AS m FROM numbers(2))
        EXCEPT ALL
        (SELECT map(toFixedString('z', 1), 'a') AS m, 2 AS n FROM numbers(2))
    )
    UNION ALL
    SELECT toLowCardinality(1) AS n, map('-1', 'b') AS m FROM numbers(2)
)
SETTINGS allow_suspicious_types_in_order_by = 1;

-- The relaxation of the constant-value comparison must apply only to the aggregate-state leaves.
-- A constant `Tuple` whose aggregate-state element is compatible between the branches but whose
-- scalar element differs holds genuinely different constants: they must not be collapsed into a
-- single header constant, and the scalar element must keep the per-branch values.
SELECT t.2 AS scalar FROM
(
    SELECT tuple(arrayReduce('quantileState(0.5)', [1]), 1) AS t
    UNION ALL
    SELECT tuple(arrayReduce('quantilesState(0.9)', [1]), 2) AS t
)
ORDER BY scalar;

-- The same for top-level constant aggregate states with different serialized state bytes: the
-- states are compatible by state representation, but they are different constants and must keep
-- the per-branch values.
SELECT finalizeAggregation(s) AS v FROM
(
    SELECT arrayReduce('quantileState(0.5)', [1]) AS s
    UNION ALL
    SELECT arrayReduce('quantileState(0.7)', [2]) AS s
)
ORDER BY v;

-- Shadowing a constant aggregate-state column with a compatible constant of a different aggregate
-- function under the same alias. The planner compares the constant values of the same-name INPUT
-- and COLUMN nodes when finalizing an actions chain step, and the plain `Field` comparison of the
-- aggregate states throws for different function names. Such constants must compare as different
-- without throwing, so that the redefinition is preserved.
SELECT finalizeAggregation(s) FROM
(
    SELECT arrayReduce('quantilesState(0.9)', [1]) AS s
    FROM
    (
        SELECT arrayReduce('quantileState(0.5)', [1]) AS s
    )
);

-- The same with different serialized state bytes: the outer redefinition must win.
SELECT finalizeAggregation(s) FROM
(
    SELECT arrayReduce('quantilesState(0.9)', [2]) AS s
    FROM
    (
        SELECT arrayReduce('quantileState(0.5)', [1]) AS s
    )
);
