-- Tags: no-random-settings
-- Regression test for convertJoinToIn bug: with `query_plan_convert_join_to_in = 1`,
-- a query `INNER JOIN ON arrayJoin(L.col) = R.col` whose SELECT (or post-JOIN
-- expression) references the source `L.col` or `arrayJoin(L.col)` again used
-- to throw `NOT_FOUND_COLUMN_IN_BLOCK` at execution. The stream after the
-- left-keys ExpressionStep contains the key expressions plus the left-header
-- columns the key DAG does not consume; columns consumed by the key
-- expression (like `L.col` under `arrayJoin(L.col)`) are gone, so the cloned
-- post-JOIN ExpressionStep referenced an INPUT the rewritten plan no longer
-- exposed. The fix declines the conversion exactly in those cases (falling
-- back to the normal JOIN plan) while keeping it for safe projections of
-- forwarded non-key columns. Co-located with #96989 (Bug A) and the JOIN-ON
-- arrayJoin duplicate-execution fix in the same family.

-- `query_plan_convert_join_to_in` and `arrayJoin` in JOIN ON are Analyzer
-- features; the old analyzer rejects `arrayJoin` in JOIN ON with
-- INVALID_JOIN_ON_EXPRESSION, so force the Analyzer here.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS lt_03918_dangling;
DROP TABLE IF EXISTS rt_03918_dangling;

CREATE TABLE lt_03918_dangling (id UInt64, tags Array(String))
ENGINE = MergeTree() ORDER BY id;
CREATE TABLE rt_03918_dangling (tag_id String)
ENGINE = MergeTree() ORDER BY tag_id;
INSERT INTO lt_03918_dangling VALUES (1, ['a','b','c']), (2, ['d','e']);
INSERT INTO rt_03918_dangling VALUES ('a'), ('d');

-- (1) Conversion stays on: SELECT references only `id`, a non-key column
--     that the post-expression stream still forwards untouched.
SELECT 'just_id', lt_03918_dangling.id
FROM lt_03918_dangling INNER JOIN rt_03918_dangling
    ON arrayJoin(lt_03918_dangling.tags) = rt_03918_dangling.tag_id
ORDER BY lt_03918_dangling.id
SETTINGS query_plan_convert_join_to_in = 1;

-- (2) SELECT references the source array column. Before fix:
--     NOT_FOUND_COLUMN_IN_BLOCK. After fix: conversion declined, query runs
--     via normal JOIN, correct result.
SELECT 'with_tags', lt_03918_dangling.id, lt_03918_dangling.tags
FROM lt_03918_dangling INNER JOIN rt_03918_dangling
    ON arrayJoin(lt_03918_dangling.tags) = rt_03918_dangling.tag_id
ORDER BY lt_03918_dangling.id
SETTINGS query_plan_convert_join_to_in = 1;

-- (3) SELECT references arrayJoin again. Same expected behaviour.
SELECT 'with_arrjoin', lt_03918_dangling.id, arrayJoin(lt_03918_dangling.tags) AS aj
FROM lt_03918_dangling INNER JOIN rt_03918_dangling
    ON arrayJoin(lt_03918_dangling.tags) = rt_03918_dangling.tag_id
ORDER BY lt_03918_dangling.id, aj
SETTINGS query_plan_convert_join_to_in = 1;

-- (4) Control: same query as (3) with conversion disabled. Confirms that
--     the fallback path returns the same answer the fix produces in (3).
SELECT 'with_arrjoin_off', lt_03918_dangling.id, arrayJoin(lt_03918_dangling.tags) AS aj
FROM lt_03918_dangling INNER JOIN rt_03918_dangling
    ON arrayJoin(lt_03918_dangling.tags) = rt_03918_dangling.tag_id
ORDER BY lt_03918_dangling.id, aj
SETTINGS query_plan_convert_join_to_in = 0;

-- (5) Plan assertions. Query (1) projects only columns the rewritten plan
--     still exposes (`id` is forwarded unchanged next to the computed
--     `arrayJoin(tags)` key), so the conversion must apply: no `Join` step,
--     a `CreatingSets` step instead.
SET explain_query_plan_default = 'legacy'; -- stable step-per-line output for the assertions below
SELECT 'explain_just_id', countIf(step LIKE 'Join%') = 0, countIf(step LIKE '%CreatingSets%') >= 1
FROM
(
    SELECT trimLeft(explain) AS step FROM
    (
        EXPLAIN description = 0
        SELECT lt_03918_dangling.id
        FROM lt_03918_dangling INNER JOIN rt_03918_dangling
            ON arrayJoin(lt_03918_dangling.tags) = rt_03918_dangling.tag_id
        ORDER BY lt_03918_dangling.id
        SETTINGS query_plan_convert_join_to_in = 1
    )
);

-- (6) Query (2) references `tags`, whose only forwarded form is
--     `arrayJoin(tags)`, so the conversion must be declined: `Join` stays.
SELECT 'explain_with_tags', countIf(step LIKE 'Join%') = 1, countIf(step LIKE '%CreatingSets%') = 0
FROM
(
    SELECT trimLeft(explain) AS step FROM
    (
        EXPLAIN description = 0
        SELECT lt_03918_dangling.id, lt_03918_dangling.tags
        FROM lt_03918_dangling INNER JOIN rt_03918_dangling
            ON arrayJoin(lt_03918_dangling.tags) = rt_03918_dangling.tag_id
        ORDER BY lt_03918_dangling.id
        SETTINGS query_plan_convert_join_to_in = 1
    )
);

DROP TABLE lt_03918_dangling;
DROP TABLE rt_03918_dangling;
