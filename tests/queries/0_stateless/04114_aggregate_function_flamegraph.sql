-- Tags: no-ordinary-database

-- flameGraph aggregate function: exercise argument arities (1/2/3),
-- merge path, empty/null traces, and error paths.
-- Addresses are picked near UInt64 max so they don't resolve to any symbol,
-- keeping the output deterministic.

SET allow_introspection_functions = 1;

SELECT '--- one argument (trace only), default size=1 ---';
SELECT arrayJoin(flameGraph(trace)) FROM (
    SELECT CAST([18446744073709551600, 18446744073709551601] AS Array(UInt64)) AS trace
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551601] AS Array(UInt64))
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551602] AS Array(UInt64))
) ORDER BY 1;

SELECT '--- two arguments (trace, size) ---';
SELECT arrayJoin(flameGraph(trace, sz)) FROM (
    SELECT CAST([18446744073709551600, 18446744073709551601] AS Array(UInt64)) AS trace, CAST(10 AS Int64) AS sz
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551601, 18446744073709551603] AS Array(UInt64)), CAST(20 AS Int64)
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551604] AS Array(UInt64)), CAST(5 AS Int64)
) ORDER BY 1;

SELECT '--- three arguments (trace, size, ptr): alloc/dealloc pairing ---';
-- Matched allocation/deallocation pairs cancel out; unmatched allocations remain.
-- ptr=10: allocated 10 then freed -10 -> cancels
-- ptr=11: allocated 20, never freed -> remains
-- ptr=12: freed without prior allocation -> ignored
SELECT arrayJoin(flameGraph(trace, sz, ptr)) FROM (
    SELECT CAST([18446744073709551600, 18446744073709551601] AS Array(UInt64)) AS trace, CAST(10 AS Int64) AS sz, CAST(10 AS UInt64) AS ptr
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551601] AS Array(UInt64)), CAST(-10 AS Int64), CAST(10 AS UInt64)
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551602] AS Array(UInt64)), CAST(20 AS Int64), CAST(11 AS UInt64)
    UNION ALL SELECT CAST([18446744073709551600, 18446744073709551604] AS Array(UInt64)), CAST(-5 AS Int64), CAST(12 AS UInt64)
) ORDER BY 1;

SELECT '--- merge: GROUP BY with two partial aggregates ---';
SELECT arrayJoin(flameGraph(trace, sz)) FROM (
    SELECT number % 2 AS g,
           CAST([18446744073709551600, 18446744073709551601 + (number % 3)] AS Array(UInt64)) AS trace,
           CAST(1 AS Int64) AS sz
    FROM numbers(20)
) GROUP BY g ORDER BY 1;

SELECT '--- trace containing a zero stops descent (find() breaks on ptr==0) ---';
SELECT arrayJoin(flameGraph(trace, sz)) FROM (
    SELECT CAST([18446744073709551600, 0, 18446744073709551602] AS Array(UInt64)) AS trace, CAST(7 AS Int64) AS sz
) ORDER BY 1;

SELECT '--- Error paths ---';
-- zero arguments
SELECT flameGraph() FROM numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- too many arguments
SELECT flameGraph(CAST([1] AS Array(UInt64)), CAST(1 AS Int64), CAST(1 AS UInt64), 1) FROM numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- wrong first argument type
SELECT flameGraph(CAST(1 AS UInt64)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT flameGraph(CAST([1] AS Array(UInt32))) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT flameGraph(CAST([1] AS Array(Int64))) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- wrong second argument type
SELECT flameGraph(CAST([1] AS Array(UInt64)), 'abc') FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT flameGraph(CAST([1] AS Array(UInt64)), CAST(1 AS UInt64)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- wrong third argument type
SELECT flameGraph(CAST([1] AS Array(UInt64)), CAST(1 AS Int64), CAST(1 AS Int64)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Parameters not allowed
SELECT flameGraph(42)(CAST([1] AS Array(UInt64))) FROM numbers(1); -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }

SELECT '--- Introspection-disabled path ---';
SET allow_introspection_functions = 0;
SELECT flameGraph(CAST([1] AS Array(UInt64))) FROM numbers(1); -- { serverError FUNCTION_NOT_ALLOWED }
