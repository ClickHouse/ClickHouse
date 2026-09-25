-- A non-literal constant aggregate-function parameter (e.g. `0.4 + 0.1`) is evaluated through the
-- `Field`-returning `evaluateConstantExpression` compatibility bridge (`materializeToField`), which
-- reads the value from the size-1 result column via `operator[]`. The parameter is routed there by
-- `getAggregateFunctionParametersArray` -> `evaluateConstantExpressionAsLiteral`, and its value is
-- directly observable in the query result. See PR #114666.
--
-- The default analyzer path always reaches the bridge for a non-literal argument (it never rewrites
-- the argument back into an `ASTLiteral`, so the literal fast path that skips the bridge does not
-- apply). The cases below therefore exercise `materializeToField` end to end.

-- Plain arithmetic results (numeric `ColumnVector`).
SELECT quantile(0.4 + 0.1)(number) FROM numbers(11);   -- level 0.5 -> 5
SELECT quantileExact(0.2 * 5)(number) FROM numbers(11); -- level 1.0 -> 10
SELECT groupArray(1 + 1)(number) FROM numbers(10);      -- max size 2 -> [0,1]

-- Non-trivial wrapped results also reach the bridge; `operator[]` flattens the wrapper.
SELECT quantile(toNullable(0.4 + 0.1))(number) FROM numbers(11);     -- Nullable(Float64) -> 5
SELECT groupArray(toNullable(1 + 1))(number) FROM numbers(10);       -- Nullable -> [0,1]
SELECT groupArray(toLowCardinality(1 + 1))(number) FROM numbers(10); -- LowCardinality -> [0,1]

-- `Bool` -> `UInt64` canonicalization: the bridge reads the value from the `Bool` result column via
-- `operator[]`, which yields `Field::UInt64`, not `Field::Bool`. `groupArray` rejects a `Field::Bool`
-- parameter (it accepts only `Int64`/`UInt64`), so a regression that stopped canonicalizing would turn
-- these into an exception instead of `[0]`.
SELECT groupArray(1 = 1)(number) FROM numbers(10);          -- comparison result (UInt8) -> [0]
SELECT groupArray(CAST(1, 'Bool'))(number) FROM numbers(10); -- genuinely Bool-typed -> [0]
