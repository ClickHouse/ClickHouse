-- A GROUP BY key that is an `if`/`multiIf` with a constant condition is collapsed to the branch it
-- always takes, so that branch is the actual grouping key and ROLLUP/CUBE/GROUPING SETS turn its
-- type Nullable under `group_by_use_nulls`. A correlated reference to it must be typed accordingly.

-- These decide whether the key is collapsed at all, and clickhouse-test randomizes them.
SET optimize_multiif_to_if = 1, optimize_if_chain_to_multiif = 0, optimize_group_by_function_keys = 1,
    optimize_injective_functions_in_group_by = 1, optimize_functions_to_subcolumns = 1,
    optimize_if_transform_strings_to_enum = 0;
SET enable_analyzer = 1, allow_experimental_correlated_subqueries = 1;

-- A correlated subquery cannot appear in ORDER BY, so the collapsing key is ordered by instead, or
-- the aggregation is wrapped. Both are needed: ROLLUP emits its grouping sets in no fixed order.

-- The reference the collapsing forms below must agree with: the same query with the bare key.
SELECT 'bare key, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH ROLLUP ORDER BY c0 NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'bare key, grouping sets', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY GROUPING SETS ((c0), ()) ORDER BY c0 NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, cube', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH CUBE
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, grouping sets', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY GROUPING SETS ((if(1, c0, false)), ())
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

-- The false branch is taken here, and it is the one that has to be registered.
SELECT 'if false branch, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(0, false, c0) WITH ROLLUP
ORDER BY if(0, false, c0) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'multiIf, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY multiIf(1, c0, false) WITH ROLLUP
ORDER BY multiIf(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

-- With the rewrite to `if` disabled the key stays a `multiIf` until it is collapsed.
SELECT 'multiIf kept, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY multiIf(1, c0, false) WITH ROLLUP
ORDER BY multiIf(1, c0, false) NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_multiif_to_if = 0;

SELECT 'if, rollup, grouping()', (SELECT c0), grouping(if(1, c0, false)) AS g
FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP ORDER BY g SETTINGS group_by_use_nulls = 1;

SELECT 'two if keys, cube', a, b FROM
(
    SELECT (SELECT c0) AS a, (SELECT c1) AS b
    FROM (SELECT 1::Bool AS c0, 0::Bool AS c1) t0
    GROUP BY if(1, c0, false), if(1, c1, true) WITH CUBE
)
ORDER BY a NULLS LAST, b NULLS LAST SETTINGS group_by_use_nulls = 1;

-- The query from the report, with the condition and the always-false branch kept as they were.
SELECT 'reported query', x, c FROM
(
    SELECT (SELECT c0) AS x, count() AS c FROM (SELECT CAST('1', 'Bool')) AS t0(c0)
    GROUP BY GROUPING SETS ((if(82, c0, (1 IN tuple(2, 2147483648, 5, toUInt128(4), 3)))), ())
)
ORDER BY x NULLS LAST SETTINGS group_by_use_nulls = 1;

-- Types other than `Bool` reach a cast wrapper that tolerates the extra Nullable layer, so they
-- returned these values before the fix as well and must keep returning them.
SELECT 'if, rollup, UInt8', (SELECT c0) FROM (SELECT 1::UInt8) t0(c0)
GROUP BY if(1, c0, 0) WITH ROLLUP
ORDER BY if(1, c0, 0) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, rollup, UInt64', (SELECT c0) FROM (SELECT 1::UInt64) t0(c0)
GROUP BY if(1, c0, 0) WITH ROLLUP
ORDER BY if(1, c0, 0) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, rollup, String', (SELECT c0) FROM (SELECT 'x'::String) t0(c0)
GROUP BY if(1, c0, 'y') WITH ROLLUP
ORDER BY if(1, c0, 'y') NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, rollup, Nullable', (SELECT c0) FROM (SELECT 1::Nullable(Bool)) t0(c0)
GROUP BY if(1, c0, false::Nullable(Bool)) WITH ROLLUP
ORDER BY if(1, c0, false::Nullable(Bool)) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if and bare key, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false), c0 WITH ROLLUP
ORDER BY c0 NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, totals', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH TOTALS
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

-- Without the setting the key keeps its own type and nothing is registered.
SELECT 'if, rollup, setting off', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 0;

-- The projection, ORDER BY and HAVING copies of the key are Nullable-converted through the same
-- lookup, so widening it must not move these values.
SELECT 'projection, rollup', if(1, c0, false) AS k, count() FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'projection, cube', if(1, c0, false) AS k, count() FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH CUBE ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'projection, grouping sets', if(1, c0, false) AS k, count() FROM (SELECT 1::Bool) t0(c0)
GROUP BY GROUPING SETS ((if(1, c0, false)), ()) ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'projection, setting off', if(1, c0, false) AS k, count() FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 0;

-- A constant-condition `if` folds to its taken constant branch during resolution, so that constant is the
-- written key and is registered as one, and a same-valued projection literal turns Nullable with it.
-- `multiIf` does not fold, so its collapsed constant is left unregistered and such a literal keeps its type.
SELECT 'constant if key', if(1, 'a', 'b') AS k, 'a' AS lit, toTypeName('a') AS lit_type
FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, 'a', 'b') WITH ROLLUP ORDER BY k NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_if_transform_strings_to_enum = 0;

SELECT 'constant if key, enum', if(1, 'a', 'b') AS k, 'a' AS lit, toTypeName('a') AS lit_type
FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, 'a', 'b') WITH ROLLUP ORDER BY k NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_if_transform_strings_to_enum = 1;

SELECT 'constant multiIf key', toTypeName(multiIf(1, 'a', c0)) AS key_type, toTypeName('a') AS lit_type
FROM (SELECT 'z'::String) t0(c0)
GROUP BY multiIf(1, 'a', c0) WITH ROLLUP ORDER BY key_type, lit_type
SETTINGS group_by_use_nulls = 1, optimize_if_transform_strings_to_enum = 0;

SELECT 'constant multiIf key, enum', toTypeName(multiIf(1, 'a', c0)) AS key_type, toTypeName('a') AS lit_type
FROM (SELECT 'z'::String) t0(c0)
GROUP BY multiIf(1, 'a', c0) WITH ROLLUP ORDER BY key_type, lit_type
SETTINGS group_by_use_nulls = 1, optimize_if_transform_strings_to_enum = 1;

-- A key that does not collapse to the correlated column keeps rejecting the query. `if(1, if(...))`
-- collapses one step, to the inner `if`, so the bare column is still not a key.
SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, if(1, c0, false), false) WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }

SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, toUInt8(c0), 0) WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }

SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY toBool(c0) WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }

SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 AND true WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }

SELECT (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY tuple(c0).1 WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_IMPLEMENTED }

SELECT (SELECT c0) FROM (SELECT 1::LowCardinality(Bool)) t0(c0)
GROUP BY if(1, c0, false::LowCardinality(Bool)) WITH ROLLUP
SETTINGS group_by_use_nulls = 1, allow_suspicious_low_cardinality_types = 1; -- { serverError NOT_IMPLEMENTED }

-- An uncorrelated reference to the column is still not an aggregate: the key list the validation
-- reads is the one written in the query, not the collapsed shape.
SELECT c0 FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(82, c0, false) WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }
