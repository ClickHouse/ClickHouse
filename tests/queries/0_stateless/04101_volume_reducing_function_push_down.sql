-- The plan-shape assertions below describe the step layout produced by the Analyzer.
-- `query_plan_merge_expressions` is pinned because the pass needs the expression and the
-- row-carrying step it is pushed below to be adjacent, which is what merging produces.
SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_merge_expressions = 1;
SET query_plan_max_step_description_length = 10000;

DROP TABLE IF EXISTS volume_reducing_function_push_down;

CREATE TABLE volume_reducing_function_push_down
(
    id UInt64,
    s String,
    fs FixedString(4),
    arr Array(UInt8),
    u UUID,
    ipv4 IPv4,
    ipv6 IPv6
)
ENGINE = Memory;

INSERT INTO volume_reducing_function_push_down VALUES
    (1, '', 'abcd', [], '00000000-0000-0000-0000-000000000001', '127.0.0.1', '::1'),
    (2, 'hello', 'xy\0\0', [1, 2, 3], '00000000-0000-0000-0000-000000000002', '127.0.0.2', '::2'),
    (3, 'привет', '\0\0\0\0', [4], '00000000-0000-0000-0000-000000000003', '127.0.0.3', '::3');

-- ----------------------------------------------------------------------------
-- Plan-shape assertions: prove the optimization actually rewrites the plan
-- (an `EXCEPT ALL` equivalence check alone cannot — it passes even when the
-- pass is a no-op). We probe each fixture with `EXPLAIN` and count the lines
-- containing `[volume-reducing functions]`.
-- ----------------------------------------------------------------------------

-- This is the shape from https://github.com/ClickHouse/ClickHouse/issues/82378: the filter needs
-- `s` to evaluate its condition, but nothing above needs it, so `length(s)` is computed before the
-- filter and `s` is not copied by it. `lengthUTF8` is deliberately excluded here because it scans
-- every input string, which can be slower than copying the strings surviving a selective filter.
SELECT 'plan: filter reading the argument — cheap function pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT length(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

SELECT 'plan: filter reading the argument — lengthUTF8 not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') = 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE like(s, 'x%')
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'plan: filter — no pushdown when disabled';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0);

SELECT 'plan: filter not reading the argument — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE id > 0
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- `trySplitFilter` also sees the expression after this outer filter is merged with the subquery.
-- The filter does not use `s`, so preserving `lengthUTF8(s)` below it would evaluate the
-- byte-scanning function for rows the filter rejects. This must not be mistaken for a previously
-- pushed function that needs to stay below the filter to prevent an optimizer cycle.
SELECT 'plan: merged filter not reading the argument — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT x FROM (SELECT id, lengthUTF8(s) AS x FROM volume_reducing_function_push_down) WHERE id > 0
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

-- The same rule applies when the predicate reads the wide argument: `lengthUTF8` scans every
-- byte and is not profitable before a potentially selective filter.
SELECT 'plan: merged filter with byte-scanning function — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT x FROM (SELECT s, lengthUTF8(s) AS x FROM volume_reducing_function_push_down) WHERE like(s, 'x%')
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- The filter already computes the same scalar. Since its predicate is not rewritten to consume
-- the pushed result, moving the selected scalar below the filter would compute it twice for rows
-- rejected by the predicate.
SELECT 'plan: scalar computed by filter — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') = 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE lengthUTF8(s) > 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- The wide column is selected as well, so it has to flow through the filter anyway and computing
-- the function earlier would only add to the data being carried.
SELECT 'plan: argument still needed above — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT s, lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- An alias of the same source still makes the wide column cross the sort. Replacing only `a`
-- would therefore evaluate `length` earlier without reducing the sort payload.
SELECT 'plan: sibling alias still needed above — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT s, length(a)
    FROM (SELECT s, s AS a, id FROM volume_reducing_function_push_down ORDER BY id)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

-- The same alias relationship can be below a plain outer `SortingStep`, whose immediate child has
-- no actions. The expression below the sort must still be checked before considering `a` replaced.
SELECT 'plan: sibling alias below outer sort — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT s, length(a)
    FROM (SELECT s, s AS a, id FROM volume_reducing_function_push_down)
    ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

-- The expression can also be separated from the outer sort by a header-preserving `LimitStep`.
-- The same source still crosses the sort as `s`, so replacing only `a` is not beneficial.
SELECT 'plan: sibling alias below outer sort and limit — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT s, length(a)
    FROM (SELECT s, s AS a, id FROM volume_reducing_function_push_down LIMIT 10)
    ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

-- `lengthUTF8` returns `UInt64`; it is not volume-reducing for a `FixedString` that occupies at
-- most eight bytes per row.
SELECT 'plan: small FixedString lengthUTF8 — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(fs) FROM volume_reducing_function_push_down ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- The positive side of the same condition: a `FixedString(64)` value is wider than the `UInt64`
-- the length functions return, so both `length` and `lengthUTF8` are volume-reducing for it. This
-- is exactly the shape measured by `tests/performance/push_down_volume_reducing_functions.xml`;
-- asserting it here makes CI fail as soon as the benchmark stops exercising the pass, instead of
-- letting it silently degrade into a timing of a plan that never changed.
DROP TABLE IF EXISTS volume_reducing_function_push_down_wide_fixed_string;

CREATE TABLE volume_reducing_function_push_down_wide_fixed_string (id UInt64, fs FixedString(64)) ENGINE = Memory;

INSERT INTO volume_reducing_function_push_down_wide_fixed_string VALUES (1, ''), (2, 'hello'), (3, 'привет');

SELECT 'plan: wide FixedString length — pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT length(fs) FROM (SELECT fs, id FROM volume_reducing_function_push_down_wide_fixed_string ORDER BY id)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'plan: wide FixedString lengthUTF8 — pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(fs) FROM (SELECT fs, id FROM volume_reducing_function_push_down_wide_fixed_string ORDER BY id)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'plan: wide FixedString lengthUTF8 — no pushdown when disabled';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(fs) FROM (SELECT fs, id FROM volume_reducing_function_push_down_wide_fixed_string ORDER BY id)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0);

-- The results must not change either.
SELECT 'result: wide FixedString lengthUTF8';
SELECT lengthUTF8(fs) FROM (SELECT fs, id FROM volume_reducing_function_push_down_wide_fixed_string ORDER BY id)
SETTINGS query_plan_push_down_volume_reducing_functions = 1;

DROP TABLE volume_reducing_function_push_down_wide_fixed_string;

-- Composite paths can share their payload with other expressions under another nested-column
-- name. Do not push them down unless the optimizer can prove the complete payload is removed.
SELECT 'plan: array length — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT length(arr) FROM volume_reducing_function_push_down ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

-- The function does not have to be an output of the parent step, but byte-scanning `lengthUTF8`
-- must not move below a `Filter`: that would scan values rejected by the filter.
SELECT 'plan: nested expression (lengthUTF8(s)+1) below filter — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) + 1 FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- A `FilterStep` is a valid parent too: its own condition is computed below the `Sorting`, so the
-- sort carries a `UInt8` instead of the `String`. `query_plan_filter_push_down = 0` keeps the
-- filter above the sort, which is what makes this shape reachable.
SELECT 'plan: filter step as parent — pushdown applied';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT id FROM (SELECT id, s FROM volume_reducing_function_push_down ORDER BY id) WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_filter_push_down = 0);

SELECT 'plan: filter step as parent — no pushdown when disabled';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT id FROM (SELECT id, s FROM volume_reducing_function_push_down ORDER BY id) WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_filter_push_down = 0);

-- `optimize_functions_to_subcolumns = 0` is pinned so that `length(s)` stays a function candidate:
-- with the subcolumn rewrite enabled there is nothing to push and the assertion would hold
-- vacuously.
SELECT 'plan: group by — barrier respected';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT length(s), count() FROM volume_reducing_function_push_down GROUP BY s
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

SELECT 'plan: distinct — barrier respected';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT length(s) FROM (SELECT DISTINCT s FROM volume_reducing_function_push_down)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0);

-- ----------------------------------------------------------------------------
-- Column-replacement assertions: the point of the optimization is that the wide column stops
-- flowing through the step, not merely that the scalar is computed earlier.
-- `query_plan_remove_unused_columns = 0` pins the check to this pass: no later column-pruning
-- pass can be credited for the difference.
-- ----------------------------------------------------------------------------

SELECT 'plan: wide column removed from the sort';
SELECT
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT lengthUTF8(s) FROM volume_reducing_function_push_down ORDER BY id
        SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_remove_unused_columns = 0)
) <
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT lengthUTF8(s) FROM volume_reducing_function_push_down ORDER BY id
        SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_remove_unused_columns = 0)
);

SELECT 'plan: wide column removed from the filter that reads it';
SELECT
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT length(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
        SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_remove_unused_columns = 0, optimize_functions_to_subcolumns = 0)
) <
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT length(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
        SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_remove_unused_columns = 0, optimize_functions_to_subcolumns = 0)
);

-- The unused `s` alias is removed before the filter is rewritten, so only `a` reaches it. The
-- optimization can therefore replace the remaining wide column with `length(a)`.
SELECT 'plan: unused sibling alias in filter — pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT length(a)
    FROM (SELECT s, s AS a FROM volume_reducing_function_push_down)
    WHERE notEmpty(a)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_remove_unused_columns = 0, optimize_functions_to_subcolumns = 0);

-- ----------------------------------------------------------------------------
-- Default-behavior regression: the existing `tryExecuteFunctionsAfterSorting`
-- (`query_plan_execute_functions_after_sorting`, on by default) must still lift non-sort
-- expressions above the `Sorting` step, even when a volume-reducing output is present. Keeping the
-- pushed functions below the sort must not disable the lift for the rest of the expression.
--
-- Probe with `fs FixedString(4)` because a `String` argument is rewritten by subcolumn pruning
-- before lift-up runs, leaving nothing to lift.
-- ----------------------------------------------------------------------------

SELECT 'plan: default lift-up not regressed when push-down disabled';
SELECT countIf(explain LIKE '%[lifted up part]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT notEmpty(fs), lower(s), upper(toString(fs)) AS sort_key FROM volume_reducing_function_push_down ORDER BY sort_key
    SETTINGS query_plan_push_down_volume_reducing_functions = 0,
             query_plan_execute_functions_after_sorting = 1);

SELECT 'plan: mixed roots keep only volume-reducing functions below sort';
SELECT countIf(explain LIKE '%[lifted up part]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT notEmpty(fs), lower(s), upper(toString(fs)) AS sort_key FROM volume_reducing_function_push_down ORDER BY sort_key
    SETTINGS query_plan_push_down_volume_reducing_functions = 1,
             query_plan_execute_functions_after_sorting = 1);

SELECT 'plan: do not push when the lifted expression needs the argument';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') = 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT notEmpty(fs), upper(toString(fs)) AS sort_key FROM volume_reducing_function_push_down ORDER BY sort_key
    SETTINGS query_plan_push_down_volume_reducing_functions = 1,
             query_plan_execute_functions_after_sorting = 1);

-- A cheap function cannot stay below a filter on its own when a sibling function on the same
-- argument scans the payload and must remain above the filter.
SELECT 'plan: mixed filter functions on one argument — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') = 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT x, y
    FROM
    (
        SELECT s, length(s) AS x, lengthUTF8(s) AS y
        FROM volume_reducing_function_push_down
    )
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1,
             optimize_functions_to_subcolumns = 0);

-- ----------------------------------------------------------------------------
-- Equivalence regressions: ON vs OFF must produce identical result sets in
-- every shape we accept for pushdown.
-- ----------------------------------------------------------------------------

SELECT 'eq: filter';
SELECT *
FROM (
    SELECT id, length(s), lengthUTF8(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, length(s), lengthUTF8(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
);

SELECT *
FROM (
    SELECT id, length(s), lengthUTF8(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, length(s), lengthUTF8(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
);

SELECT 'eq: unsupported UUID/IP roots';
SELECT *
FROM (
    SELECT id, empty(u), empty(ipv4), empty(ipv6)
    FROM volume_reducing_function_push_down
    ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, empty(u), empty(ipv4), empty(ipv6)
    FROM volume_reducing_function_push_down
    ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
);

SELECT *
FROM (
    SELECT id, empty(u), empty(ipv4), empty(ipv6)
    FROM volume_reducing_function_push_down
    ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, empty(u), empty(ipv4), empty(ipv6)
    FROM volume_reducing_function_push_down
    ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
);

SELECT 'eq: sort+limit';
SELECT *
FROM (
    SELECT id, length(s), length(arr), empty(arr), notEmpty(fs)
    FROM volume_reducing_function_push_down
    ORDER BY id DESC
    LIMIT 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, length(s), length(arr), empty(arr), notEmpty(fs)
    FROM volume_reducing_function_push_down
    ORDER BY id DESC
    LIMIT 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
);

SELECT *
FROM (
    SELECT id, length(s), length(arr), empty(arr), notEmpty(fs)
    FROM volume_reducing_function_push_down
    ORDER BY id DESC
    LIMIT 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, length(s), length(arr), empty(arr), notEmpty(fs)
    FROM volume_reducing_function_push_down
    ORDER BY id DESC
    LIMIT 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
);

SELECT 'eq: nested expression';
SELECT *
FROM (
    SELECT lengthUTF8(s) + 1, length(s) * 2
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    ORDER BY 1
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT lengthUTF8(s) + 1, length(s) * 2
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    ORDER BY 1
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
);

SELECT *
FROM (
    SELECT lengthUTF8(s) + 1, length(s) * 2
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    ORDER BY 1
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT lengthUTF8(s) + 1, length(s) * 2
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    ORDER BY 1
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
);

SELECT 'eq: filter step as parent';
SELECT *
FROM (
    SELECT id FROM (SELECT id, s FROM volume_reducing_function_push_down ORDER BY id) WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_filter_push_down = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id FROM (SELECT id, s FROM volume_reducing_function_push_down ORDER BY id) WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_filter_push_down = 0
);

SELECT *
FROM (
    SELECT id FROM (SELECT id, s FROM volume_reducing_function_push_down ORDER BY id) WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_filter_push_down = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id FROM (SELECT id, s FROM volume_reducing_function_push_down ORDER BY id) WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_filter_push_down = 0
);

SELECT 'eq: group by';
SELECT length(s), count()
FROM volume_reducing_function_push_down
GROUP BY s
ORDER BY length(s)
SETTINGS query_plan_push_down_volume_reducing_functions = 1;

SELECT 'eq: join';
SELECT length(l.s), r.id
FROM volume_reducing_function_push_down AS l
INNER JOIN volume_reducing_function_push_down AS r ON l.s = r.s
ORDER BY r.id
SETTINGS query_plan_push_down_volume_reducing_functions = 1;

SELECT 'eq: distinct subquery';
SELECT *
FROM (
    SELECT length(s) FROM (SELECT DISTINCT s FROM volume_reducing_function_push_down)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s) FROM (SELECT DISTINCT s FROM volume_reducing_function_push_down)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, optimize_functions_to_subcolumns = 0
);

SELECT *
FROM (
    SELECT length(s) FROM (SELECT DISTINCT s FROM volume_reducing_function_push_down)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, optimize_functions_to_subcolumns = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s) FROM (SELECT DISTINCT s FROM volume_reducing_function_push_down)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 0
);

SELECT 'eq: distinct union subquery';
SELECT *
FROM (
    SELECT length(s)
    FROM
    (
        SELECT DISTINCT s FROM volume_reducing_function_push_down
        UNION ALL
        SELECT DISTINCT s FROM volume_reducing_function_push_down
    )
    SETTINGS query_plan_push_down_volume_reducing_functions = 1,
             optimize_functions_to_subcolumns = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s)
    FROM
    (
        SELECT DISTINCT s FROM volume_reducing_function_push_down
        UNION ALL
        SELECT DISTINCT s FROM volume_reducing_function_push_down
    )
    SETTINGS query_plan_push_down_volume_reducing_functions = 0,
             optimize_functions_to_subcolumns = 0
);

SELECT *
FROM (
    SELECT length(s)
    FROM
    (
        SELECT DISTINCT s FROM volume_reducing_function_push_down
        UNION ALL
        SELECT DISTINCT s FROM volume_reducing_function_push_down
    )
    SETTINGS query_plan_push_down_volume_reducing_functions = 0,
             optimize_functions_to_subcolumns = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s)
    FROM
    (
        SELECT DISTINCT s FROM volume_reducing_function_push_down
        UNION ALL
        SELECT DISTINCT s FROM volume_reducing_function_push_down
    )
    SETTINGS query_plan_push_down_volume_reducing_functions = 1,
             optimize_functions_to_subcolumns = 0
);

-- ----------------------------------------------------------------------------
-- Name-collision regression: when the pushed scalar's output name equals a surviving passthrough
-- column (here `length(s) AS id` aliased onto the table's own `id`, kept as the sort key),
-- name-based resolution must not bind the parent input to the original column.
-- ----------------------------------------------------------------------------

SELECT 'eq: alias collides with sort key';
SELECT *
FROM (
    SELECT length(s) AS id
    FROM volume_reducing_function_push_down AS t
    ORDER BY t.id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s) AS id
    FROM volume_reducing_function_push_down AS t
    ORDER BY t.id
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
);

SELECT *
FROM (
    SELECT length(s) AS id
    FROM volume_reducing_function_push_down AS t
    ORDER BY t.id
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s) AS id
    FROM volume_reducing_function_push_down AS t
    ORDER BY t.id
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
);

-- Stateful and non-deterministic functions in the same expression are not moved: only the
-- deterministic volume-reducing function goes below the child step.
SELECT 'eq: stateful function in the same expression';
SELECT *
FROM (
    SELECT length(s), rowNumberInAllBlocks()
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    ORDER BY 1, 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, max_threads = 1, max_block_size = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT length(s), rowNumberInAllBlocks()
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    ORDER BY 1, 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, max_threads = 1, max_block_size = 1
);

-- ----------------------------------------------------------------------------
-- Convergence regressions: shapes on which the pass used to ping-pong with
-- `mergeExpressions` and `trySplitFilter` / `tryExecuteFunctionsAfterSorting` until
-- `TOO_MANY_QUERY_PLAN_OPTIMIZATIONS` was thrown.
-- ----------------------------------------------------------------------------

-- The pushed function is merged into the `Filter`, whose condition also reads the argument.
-- `trySplitFilter` must keep the function in the filter part instead of lifting it back above
-- the filter, where the pass would push it down again, forever.
SELECT 'converges: filter condition reading the argument';
SELECT s != '' FROM volume_reducing_function_push_down WHERE s < 'zzzzzzzz' ORDER BY id;

-- The pushed function is merged with the expression below the `Sorting` that computes its
-- argument. `tryExecuteFunctionsAfterSorting` must keep the function below the sort even though
-- the argument is a computed column of the merged expression, not an input.
SELECT 'converges: argument computed below the sorting';
SELECT length(s2) FROM (SELECT concat(s, '!') AS s2, id FROM volume_reducing_function_push_down ORDER BY id);

DROP TABLE volume_reducing_function_push_down;
