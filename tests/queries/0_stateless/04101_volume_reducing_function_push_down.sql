-- The plan-shape assertions below describe the step layout produced by the
-- new analyzer; with the old analyzer the projection expressions are computed
-- below the Sorting step already and the pass (correctly) never fires.
SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_max_step_description_length = 10000;

DROP TABLE IF EXISTS volume_reducing_function_push_down;

CREATE TABLE volume_reducing_function_push_down
(
    id UInt64,
    s String,
    fs FixedString(4),
    arr Array(UInt8)
)
ENGINE = Memory;

INSERT INTO volume_reducing_function_push_down VALUES
    (1, '', 'abcd', []),
    (2, 'hello', 'xy\0\0', [1, 2, 3]),
    (3, 'привет', '\0\0\0\0', [4]);

-- ----------------------------------------------------------------------------
-- Plan-shape assertions: prove the optimization actually rewrites the plan
-- (an `EXCEPT ALL` equivalence check alone cannot — it passes even when the
-- pass is a no-op). We probe each fixture with `EXPLAIN`, count the lines
-- containing `[volume-reducing functions]`, and compare ON vs OFF.
-- ----------------------------------------------------------------------------

SELECT 'plan: filter — pushdown applied';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 0);

SELECT 'plan: filter — no pushdown when disabled';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1
    SELECT lengthUTF8(s) FROM volume_reducing_function_push_down WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_merge_expressions = 0);

SELECT 'plan: sort+limit — pushdown applied';
SELECT countIf(explain LIKE '%[volume-reducing functions]%') > 0
FROM (EXPLAIN description = 1
    SELECT length(s), length(arr), empty(arr), notEmpty(fs) FROM volume_reducing_function_push_down ORDER BY id DESC LIMIT 2
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 0);

-- The probe computes `length(s)` above a DISTINCT subquery and asserts on the
-- marker's *position*: the pass walks the function down the Expression chain
-- (`Projection` → `Change column names` → subquery `Project names`) and must
-- stop at the `Distinct`, so markers may appear above it (proving the pass was
-- active, not vacuous) but never below it. A plain whole-plan marker count
-- cannot express this — the number of Expression steps above the barrier is
-- a plan-shape detail that varies with settings.
-- `optimize_functions_to_subcolumns = 0` is pinned so that `length(s)` stays
-- a FUNCTION candidate: with the subcolumn rewrite enabled there is nothing
-- to push and the assertion would hold vacuously (this is also what made the
-- previous formulation flip under randomized settings).
SELECT 'plan: distinct — barrier respected';
SELECT (countIf(marker AND (rn < first_distinct)) > 0) AND (countIf(marker AND (rn > first_distinct)) = 0)
FROM (
    SELECT explain LIKE '%[volume-reducing functions]%' AS marker,
           rn,
           minIf(rn, explain LIKE '%Distinct%') OVER () AS first_distinct
    FROM (SELECT explain, rowNumberInAllBlocks() AS rn FROM (EXPLAIN description = 1
        SELECT length(s) FROM (SELECT DISTINCT s FROM volume_reducing_function_push_down)
        SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 0,
                 optimize_functions_to_subcolumns = 0))
);

SELECT 'plan: group by — barrier respected';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1
    SELECT length(s), count() FROM volume_reducing_function_push_down GROUP BY s
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 0);

SELECT 'plan: nested expression (length(s)+1) — not pushed';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1
    SELECT length(s) + 1 FROM volume_reducing_function_push_down WHERE id > 0
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 0);

-- ----------------------------------------------------------------------------
-- Header-shape assertion: for a pure-passthrough child (`Sort` / `Limit`) the
-- wide source column must be dropped from the step, not merely carried through.
-- We probe with the `fs FixedString(4)` column on purpose: ClickHouse's earlier
-- subcolumn-pruning pass rewrites `length(s)` / `length(arr)` to read the
-- `s.size` / `arr.size0` subcolumn, so a `String` / `Array` argument never
-- actually flows into `Sorting` / `Limit` regardless of this optimization.
-- `FixedString` has no `.size` subcolumn (the size is part of the type), so
-- `notEmpty(fs)` is the smallest probe that observes the wide-column pruning
-- this optimization is responsible for. With the optimization ON it survives
-- only below the pushed step; with it OFF it also flows through `Sorting`
-- and `Limit`. So the ON plan must mention `fs FixedString` strictly fewer
-- times than the OFF plan.
-- ----------------------------------------------------------------------------

SELECT 'header: wide column pruned from sort/limit when enabled';
SELECT
    (SELECT countIf(explain LIKE '%fs FixedString%')
     FROM (EXPLAIN header = 1
        SELECT notEmpty(fs) FROM volume_reducing_function_push_down ORDER BY id DESC LIMIT 2
        SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 1))
    <
    (SELECT countIf(explain LIKE '%fs FixedString%')
     FROM (EXPLAIN header = 1
        SELECT notEmpty(fs) FROM volume_reducing_function_push_down ORDER BY id DESC LIMIT 2
        SETTINGS query_plan_push_down_volume_reducing_functions = 0, query_plan_merge_expressions = 1));

-- ----------------------------------------------------------------------------
-- Default-behavior regression: when the optimization is disabled, the existing
-- `tryExecuteFunctionsAfterSorting` (`query_plan_execute_functions_after_sorting`,
-- on by default) must still lift non-sort expressions above the `Sorting` step,
-- even when a volume-reducing output is present. Guarding the lift on the new
-- setting must not change the default plan.
--
-- Probe with `fs FixedString(4)` for the same reason as the header assertion
-- above: a `String` argument is rewritten by subcolumn pruning before lift-up
-- runs, leaving nothing to lift. `merge_expressions = 1` is required so the
-- per-step expressions land in a shape `tryExecuteFunctionsAfterSorting`
-- recognises (with merge OFF, `unneeded_for_sorting.trivial()` short-circuits
-- and lift-up never fires regardless of this optimization).
-- ----------------------------------------------------------------------------

SELECT 'plan: default lift-up not regressed when push-down disabled';
SELECT countIf(explain LIKE '%[lifted up part]%') > 0
FROM (EXPLAIN description = 1
    SELECT notEmpty(fs), upper(toString(fs)) FROM volume_reducing_function_push_down ORDER BY id
    SETTINGS query_plan_push_down_volume_reducing_functions = 0,
             query_plan_execute_functions_after_sorting = 1,
             query_plan_merge_expressions = 1);

-- ----------------------------------------------------------------------------
-- Equivalence regressions: ON vs OFF must produce identical result sets in
-- every shape we accept for pushdown.
-- ----------------------------------------------------------------------------

SELECT 'eq: filter';
SELECT *
FROM (
    SELECT id, length(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 1
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, length(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
);

SELECT *
FROM (
    SELECT id, length(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
    SETTINGS query_plan_push_down_volume_reducing_functions = 0
)
EXCEPT ALL
SELECT *
FROM (
    SELECT id, length(s)
    FROM volume_reducing_function_push_down
    WHERE notEmpty(s)
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

-- ----------------------------------------------------------------------------
-- Name-collision regression: when the pushed scalar's output name equals a
-- surviving passthrough column (here `length(s) AS id` aliased onto the table's
-- own `id`, kept as the sort key), name-based resolution must not bind the
-- parent input to the original column. ON vs OFF must stay identical.
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

DROP TABLE volume_reducing_function_push_down;
