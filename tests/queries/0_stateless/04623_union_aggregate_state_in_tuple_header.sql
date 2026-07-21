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
