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
