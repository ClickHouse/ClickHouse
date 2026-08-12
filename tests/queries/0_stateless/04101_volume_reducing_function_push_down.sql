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
-- `s` to evaluate its condition, but nothing above needs it, so `lengthUTF8(s)` is computed before
-- the filter and `s` is not copied by it.
SELECT 'plan: filter reading the argument — pushdown applied';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'plan: filter — no pushdown when disabled';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0);

SELECT 'plan: filter not reading the argument — pushdown applied';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE id > 0
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- The wide column is selected as well, so it has to flow through the filter anyway and computing
-- the function earlier would only add to the data being carried.
SELECT 'plan: argument still needed above — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT s, lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1);

-- The function does not have to be an output of the parent step: `ActionsDAG::split` leaves the
-- enclosing expression in place and only moves `lengthUTF8(s)` down.
SELECT 'plan: nested expression (lengthUTF8(s)+1) — pushdown applied';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
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

SELECT 'plan: wide column removed from the filter';
SELECT
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE id > 0
        SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_remove_unused_columns = 0)
) <
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE id > 0
        SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_remove_unused_columns = 0)
);

SELECT 'plan: wide column removed from the filter that reads it';
SELECT
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
        SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_remove_unused_columns = 0)
) <
(
    SELECT countIf(explain LIKE '%s String%')
    FROM (EXPLAIN header = 1, description = 0, actions = 0, compact = 0, pretty = 0
        SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
        SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_remove_unused_columns = 0)
);

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

SELECT 'plan: unsupported UUID/IP roots still lift up';
SELECT countIf(explain LIKE '%[lifted up part]%') > 0
FROM (EXPLAIN description = 1, actions = 0, compact = 0, pretty = 0
    SELECT empty(u), empty(ipv4), empty(ipv6), upper(toString(fs)) AS sort_key FROM volume_reducing_function_push_down ORDER BY sort_key
    SETTINGS query_plan_push_down_volume_reducing_functions = 1,
             query_plan_execute_functions_after_sorting = 1);

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

DROP TABLE volume_reducing_function_push_down;
