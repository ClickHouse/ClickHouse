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

SELECT 'plan: distinct — barrier respected';
SELECT countIf(explain LIKE '%[volume-reducing functions]%')
FROM (EXPLAIN description = 1
    SELECT DISTINCT length(s) FROM volume_reducing_function_push_down
    SETTINGS query_plan_push_down_volume_reducing_functions = 1, query_plan_merge_expressions = 0);

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

DROP TABLE volume_reducing_function_push_down;
