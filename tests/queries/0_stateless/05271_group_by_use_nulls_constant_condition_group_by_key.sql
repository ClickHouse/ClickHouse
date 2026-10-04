-- A GROUP BY key `if`/`multiIf` with a constant condition always evaluates to one of its branches. When that
-- branch is a column, the key read back by other clauses and a correlated subquery reading the column must return
-- what they return when the column itself is the key, including the NULLs of ROLLUP/CUBE/GROUPING SETS under
-- `group_by_use_nulls`.

-- clickhouse-test randomizes these; they are pinned at their defaults to keep the query shapes below.
SET optimize_multiif_to_if = 1, optimize_if_chain_to_multiif = 0, optimize_group_by_function_keys = 1,
    optimize_injective_functions_in_group_by = 1, optimize_functions_to_subcolumns = 1,
    optimize_if_transform_strings_to_enum = 0;
SET allow_correlated_subqueries = 1;

DROP DICTIONARY IF EXISTS d05271;
CREATE DICTIONARY d05271 (id UInt64, v String) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);

-- A correlated subquery cannot appear in ORDER BY, so the collapsing key is ordered by instead, or
-- the aggregation is wrapped. Both are needed: ROLLUP emits its grouping sets in no fixed order.

-- The reference the collapsing forms below must agree with: the same query with the bare key.
SELECT 'bare key, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH ROLLUP ORDER BY c0 NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'if, cube', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH CUBE
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

-- The false branch is taken here.
SELECT 'if false branch, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(0, false, c0) WITH ROLLUP
ORDER BY if(0, false, c0) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'multiIf, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY multiIf(1, c0, false) WITH ROLLUP
ORDER BY multiIf(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

-- The same with the rewrite of `multiIf` to `if` disabled.
SELECT 'multiIf kept, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY multiIf(1, c0, false) WITH ROLLUP
ORDER BY multiIf(1, c0, false) NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_multiif_to_if = 0;

SELECT 'nested if, rollup', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, if(1, c0, false), false) WITH ROLLUP
ORDER BY if(1, if(1, c0, false), false) NULLS LAST SETTINGS group_by_use_nulls = 1;

-- The condition is itself a `multiIf` with a constant condition.
SELECT 'multiIf condition, rollup', (SELECT c0) FROM (SELECT 1::Bool, 1::UInt8) t0(c0, c1)
GROUP BY if(multiIf(1, 1, c1), c0, false) WITH ROLLUP
ORDER BY if(multiIf(1, 1, c1), c0, false) NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'correlated, collapsed column repeated', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, not(c0)) WITH ROLLUP
ORDER BY if(1, c0, not(c0)) NULLS LAST SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 1;

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

-- Types other than `Bool` returned these values before the fix as well and must keep returning them.
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

-- Without the setting the key keeps its own type.
SELECT 'if, rollup, setting off', (SELECT c0) FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(1, c0, false) WITH ROLLUP
ORDER BY if(1, c0, false) NULLS LAST SETTINGS group_by_use_nulls = 0;

-- Keys in which the collapsed column occurs more than once, with enable_identifier_resolve_cache on (the default).
SELECT 'collapsed column repeated', if(1, c0, lower(c0)) AS k, count() FROM (SELECT 'x'::String) t0(c0)
GROUP BY k WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 1;

SELECT 'collapsed column repeated, written sub-key', if(1, c0, lower(c0)) AS k, count() FROM (SELECT 'x'::String) t0(c0)
GROUP BY lower(c0), k WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 1;

SELECT 'nested, collapsed column repeated', if(0, c0, if(0, lower(c0), c0)) AS k, count() FROM (SELECT 'x'::String) t0(c0)
GROUP BY lower(c0), k WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1, enable_identifier_resolve_cache = 1;

-- The branch that is not taken reads a column that is not a key.
SELECT 'other branch not a key', multiIf(0, b, a) AS k, count() FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0
GROUP BY k WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT 'nested, other branch not a key', if(0, b, if(0, b, a)) AS k, count() FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0
GROUP BY k WITH ROLLUP ORDER BY k NULLS LAST SETTINGS group_by_use_nulls = 1;

-- A key that collapses to another key keeps its own grouping sets.
SELECT 'collapses to another key, rollup', a, if(0, b, if(0, b, a)) AS k, grouping(a, k), count()
FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0 GROUP BY ROLLUP(a, k) ORDER BY a, k;

SELECT 'collapses to another key, cube', a, if(0, b, if(0, b, a)) AS k, count()
FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0 GROUP BY CUBE(a, k) ORDER BY a, k;

SELECT 'collapses to another key, grouping sets', a, if(0, b, if(0, b, a)) AS k, count()
FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0 GROUP BY GROUPING SETS ((a), (k)) ORDER BY a, k;

SELECT 'collapses to another key, rollup, nulls', a, if(0, b, if(0, b, a)) AS k, count()
FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0 GROUP BY ROLLUP(a, k) ORDER BY a NULLS LAST, k NULLS LAST
SETTINGS group_by_use_nulls = 1;

-- The same with `if` chains rewritten to `multiIf`.
SELECT 'collapses to another key, rollup, nulls, multiIf', a, if(0, b, if(0, b, a)) AS k, count()
FROM (SELECT 'x'::String AS a, 'y'::String AS b) t0 GROUP BY ROLLUP(a, k) ORDER BY a NULLS LAST, k NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;

-- Also when the conditions are not constant, and when they read other keys.
SELECT 'if chain, rollup, nulls, multiIf', if(number > 1, 'p', if(number > 0, 'q', 'r')) AS k, concat(k, 'z'), count()
FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY k NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;

SELECT 'if chain of other keys, rollup, nulls, multiIf', a, b, if(a = 'x', a, if(b = 'y', b, a)) AS k, count()
FROM (SELECT 'x' AS a, 'w' AS b) t0 GROUP BY ROLLUP(a, b, k) ORDER BY ALL
SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;

-- Columns of different sources are different keys, in the branch that is taken and in the ones that are not.
SELECT 'collapses to another key, other sources, cube', if(0, s2.b, if(0, s2.b, s1.a)) AS k1, if(0, s3.b, if(0, s3.b, s1.a)) AS k2, count()
FROM (SELECT 'x' AS a) AS s1, (SELECT 'y' AS b) AS s2, (SELECT 'y' AS b) AS s3 GROUP BY CUBE(k1, k2) ORDER BY k1, k2;

SELECT 'collapses to columns of other sources, rollup', a, c FROM
(
    SELECT (SELECT s1.a) AS a, count() AS c FROM (SELECT 1::UInt8 AS a) AS s1, (SELECT 1::UInt8 AS a) AS s2
    GROUP BY ROLLUP(if(1, s1.a, 0), if(1, s2.a, 0))
)
ORDER BY a, c;

-- The same in a subquery of such a query.
SELECT 'subquery of a query with a key that collapses to another key', a, count() FROM
(
    SELECT (SELECT c0) AS a FROM (SELECT 1::UInt8) t0(c0)
    GROUP BY if(1, c0, 0) WITH ROLLUP
)
GROUP BY ROLLUP(a, if(0, a, if(0, a, a))) ORDER BY ALL SETTINGS group_by_use_nulls = 1;

-- Keys that collapse to the same column and are in the same grouping sets.
SELECT 'keys collapse together, grouping sets', (SELECT c0) FROM (SELECT 1::UInt8) t0(c0)
GROUP BY GROUPING SETS ((if(1, c0, 0), if(1, c0, 2))) SETTINGS group_by_use_nulls = 1;

SELECT 'keys collapse together, grouping sets, setting off', a, c FROM
(
    SELECT (SELECT c0) AS a, count() AS c FROM (SELECT 1::UInt8) t0(c0)
    GROUP BY GROUPING SETS ((if(1, c0, 0), if(1, c0, 2)), ())
)
ORDER BY a SETTINGS group_by_use_nulls = 0;

-- The condition becomes constant only when the dictionary lookup, which matches no key, is optimized away.
SELECT 'dictionary condition, rollup', (SELECT c0) FROM (SELECT 1::Bool AS c0, 1::UInt64 AS id) t0
GROUP BY if(dictGet('d05271', 'v', id) = 'x', false, c0) WITH ROLLUP
ORDER BY if(dictGet('d05271', 'v', id) = 'x', false, c0) NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 1;

SELECT 'nested dictionary condition, rollup', (SELECT c0) FROM (SELECT 1::Bool AS c0, 1::UInt64 AS id) t0
GROUP BY if(1, if(dictGet('d05271', 'v', id) = 'x', false, c0), false) WITH ROLLUP
ORDER BY if(1, if(dictGet('d05271', 'v', id) = 'x', false, c0), false) NULLS LAST
SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 1;

-- A key that does not collapse to the correlated column keeps rejecting the query.
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

SELECT (SELECT c0) FROM (SELECT 1::Bool AS c0, 1::UInt64 AS id) t0
GROUP BY if(1, if(dictGet('d05271', 'v', id) = 'x', false, c0), false) WITH ROLLUP
SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 0; -- { serverError NOT_IMPLEMENTED }

-- An uncorrelated reference to the column is still rejected: the column is not one of the keys as written.
SELECT c0 FROM (SELECT 1::Bool) t0(c0)
GROUP BY if(82, c0, false) WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }

DROP DICTIONARY d05271;
