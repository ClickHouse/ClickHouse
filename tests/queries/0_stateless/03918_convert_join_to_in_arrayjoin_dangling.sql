-- Tags: no-random-settings
-- Regression test for convertJoinToIn bug: with `query_plan_convert_join_to_in = 1`,
-- a query `INNER JOIN ON arrayJoin(L.col) = R.col` whose SELECT (or post-JOIN
-- expression) references the source `L.col` or `arrayJoin(L.col)` again used
-- to throw NOT_FOUND_COLUMN_IN_BLOCK at execution. `left_pre_join_actions`
-- only forwards the JOIN keys, so the cloned post-JOIN ExpressionStep
-- referenced an INPUT (`L.col`) that the rewritten plan no longer exposed.
-- The fix declines the conversion in those cases and falls back to the
-- normal JOIN plan, which is correct. Co-located with #96989 (Bug A) and
-- the JOIN-ON arrayJoin duplicate-execution fix in the same family.

DROP TABLE IF EXISTS lt_03918_dangling;
DROP TABLE IF EXISTS rt_03918_dangling;

CREATE TABLE lt_03918_dangling (id UInt64, tags Array(String))
ENGINE = MergeTree() ORDER BY id;
CREATE TABLE rt_03918_dangling (tag_id String)
ENGINE = MergeTree() ORDER BY tag_id;
INSERT INTO lt_03918_dangling VALUES (1, ['a','b','c']), (2, ['d','e']);
INSERT INTO rt_03918_dangling VALUES ('a'), ('d');

-- (1) Conversion stays on: SELECT references only the JOIN key column.
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

DROP TABLE lt_03918_dangling;
DROP TABLE rt_03918_dangling;
