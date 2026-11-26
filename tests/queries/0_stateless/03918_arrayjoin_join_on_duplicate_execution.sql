-- Tags: long, no-random-settings
-- Regression test for oncall 62315: `arrayJoin()` inside JOIN ON was
-- executed twice in the new logical-join-step plan, producing rowcount
-- multiplied by |array| an extra time. Every query pins
-- `query_plan_use_new_logical_join_step` (and any other optimizer toggle
-- it depends on) explicitly so CI random-settings cannot move the query
-- off the target path.

set explain_query_plan_default='legacy';

DROP TABLE IF EXISTS items_arrjoin;
DROP TABLE IF EXISTS tags_arrjoin;
DROP TABLE IF EXISTS right_arrjoin;

CREATE TABLE items_arrjoin (id UInt64, filter_key String, tag_ids Array(String))
ENGINE = MergeTree() ORDER BY (filter_key, id);
CREATE TABLE tags_arrjoin (id String, name String)
ENGINE = MergeTree() ORDER BY id;

INSERT INTO tags_arrjoin SELECT concat('tag_', toString(number)), '' FROM numbers(10);
INSERT INTO items_arrjoin
SELECT number, 'target',
       arrayMap(i -> concat('tag_', toString((number * 3 + i) % 10)), range(3))
FROM numbers(2);

-- (1) Baseline. Bug = 18; fix = 6 (= 2 items * 3 tags).
SELECT 'baseline_new=1', count() FROM items_arrjoin INNER JOIN tags_arrjoin
    ON arrayJoin(items_arrjoin.tag_ids) = tags_arrjoin.id
    WHERE items_arrjoin.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 1;

-- (2) Control: old path.
SELECT 'baseline_new=0', count() FROM items_arrjoin INNER JOIN tags_arrjoin
    ON arrayJoin(items_arrjoin.tag_ids) = tags_arrjoin.id
    WHERE items_arrjoin.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 0;

-- (3) Reversed equality direction.
SELECT 'reversed_new=1', count() FROM items_arrjoin INNER JOIN tags_arrjoin
    ON tags_arrjoin.id = arrayJoin(items_arrjoin.tag_ids)
    WHERE items_arrjoin.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 1;

-- (4) Nested arrayJoin in JOIN ON.
SELECT 'nested_arr_new=1', count() FROM items_arrjoin INNER JOIN tags_arrjoin
    ON toString(arrayJoin(items_arrjoin.tag_ids)) = tags_arrjoin.id
    WHERE items_arrjoin.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 1;

-- (5) MECHANISM-LEVEL: count ARRAY JOIN actions in plan. The substring
--     `'ARRAY JOIN '` (with trailing space) matches every `ARRAY_JOIN`
--     action line regardless of internal `__tableN.colname` aliasing,
--     so this assertion does not bind to internal alias text and stays
--     stable across renderer / alias-naming churn. Bug = 2; fix = 1.
SELECT 'arrjoin_count_in_plan_new=1',
    countSubstrings(arrayStringConcat(groupArray(explain), char(10)),
                    'ARRAY JOIN ')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM items_arrjoin INNER JOIN tags_arrjoin
        ON arrayJoin(items_arrjoin.tag_ids) = tags_arrjoin.id
        WHERE items_arrjoin.filter_key = 'target'
        SETTINGS query_plan_use_new_logical_join_step = 1
);

-- (6) AND-arrayJoin conjunct -- mechanism-level plan-shape check that
--     simultaneously validates two invariants:
--     (a) the ON-side arrayJoin ghost is removed by the fix (calls
--         `fix_predicate_for_join_logical_step` -> `removeNodes`).
--     (b) the WHERE-side arrayJoin conjunct stays ABOVE the JOIN
--         (upstream invariant `splitActionsForJOINFilterPushDown`
--         rejects ARRAY_JOIN from `allowed_nodes`, so it never enters
--         the pushed-down filter).
--     Expected plan-shape count = 2: ON arrayJoin (1) + WHERE arrayJoin
--     above JOIN (1). Bug-shape = 3: ON arrayJoin gets ghosted in the
--     pushed-down filter -> 2 there + 1 WHERE above JOIN. If a future
--     over-aggressive fix also dropped the WHERE arrayJoin, this count
--     would collapse to 1 and this assertion would catch it.
--     Per ClickHouse semantics, count() of the SELECT itself is NOT a
--     fix-vs-bug signal here because each arrayJoin performs its own
--     row expansion (see 03832_array_join_filter_push_down_cross_join.sql).
SELECT 'and_conjunct_arrjoin_count_in_plan_new=1',
    countSubstrings(arrayStringConcat(groupArray(explain), char(10)),
                    'ARRAY JOIN ')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM items_arrjoin INNER JOIN tags_arrjoin
        ON arrayJoin(items_arrjoin.tag_ids) = tags_arrjoin.id
        WHERE items_arrjoin.filter_key = 'target'
          AND arrayJoin(items_arrjoin.tag_ids) <> ''
        SETTINGS query_plan_use_new_logical_join_step = 1
);

-- (7) Subquery (control: avoids buggy plan shape).
SELECT 'control_subquery_new=1', count() FROM (
    SELECT id, arrayJoin(tag_ids) AS tag_id
    FROM items_arrjoin WHERE filter_key = 'target') AS sub
INNER JOIN tags_arrjoin ON sub.tag_id = tags_arrjoin.id
SETTINGS query_plan_use_new_logical_join_step = 1;

-- (8) Runtime filters off.
SELECT 'no_rt_filter_new=1', count() FROM items_arrjoin INNER JOIN tags_arrjoin
    ON arrayJoin(items_arrjoin.tag_ids) = tags_arrjoin.id
    WHERE items_arrjoin.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 1, enable_join_runtime_filters = 0;

-- (9) Disjunction push-down off.
SELECT 'no_disj_pd_new=1', count() FROM items_arrjoin INNER JOIN tags_arrjoin
    ON arrayJoin(items_arrjoin.tag_ids) = tags_arrjoin.id
    WHERE items_arrjoin.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 1, use_join_disjunctions_push_down = 0;

-- (10)-(12) BILATERAL arrayJoin: both sides of JOIN ON use arrayJoin.
--      Covers THREE WHERE shapes so both left- and right-side
--      `fix_predicate_for_join_logical_step` invocations are exercised:
--      asymmetric WHERE on the left only, asymmetric WHERE on the right
--      only, and separable WHERE on both sides simultaneously.
--      Plan-shape assertions count `'ARRAY JOIN '` regardless of side
--      alias so they don't bind to internal `__tableN.col` text.
CREATE TABLE right_arrjoin (id UInt64, filter_key String, arr Array(String))
ENGINE = MergeTree() ORDER BY (filter_key, id);
INSERT INTO right_arrjoin
SELECT number, 'rtarget',
       arrayMap(i -> concat('tag_', toString((number * 3 + i) % 10)), range(3))
FROM numbers(2);

-- (10) Bilateral, WHERE on LEFT only -- left-side push-down triggers.
SELECT 'bilateral_left_where_count_new=1', count() FROM items_arrjoin AS L
    INNER JOIN right_arrjoin AS R
    ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
    WHERE L.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 1;
SELECT 'bilateral_left_where_count_new=0', count() FROM items_arrjoin AS L
    INNER JOIN right_arrjoin AS R
    ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
    WHERE L.filter_key = 'target'
    SETTINGS query_plan_use_new_logical_join_step = 0;
SELECT 'bilateral_left_where_arrjoin_count_in_plan_new=1',
    countSubstrings(arrayStringConcat(groupArray(explain), char(10)),
                    'ARRAY JOIN ')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM items_arrjoin AS L
        INNER JOIN right_arrjoin AS R
        ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
        WHERE L.filter_key = 'target'
        SETTINGS query_plan_use_new_logical_join_step = 1
);

-- (11) Bilateral, WHERE on RIGHT only -- right-side push-down triggers.
--      Symmetric coverage to (10); catches a right-side-only bug in
--      `removeNodes` that left-only WHERE cannot expose.
SELECT 'bilateral_right_where_count_new=1', count() FROM items_arrjoin AS L
    INNER JOIN right_arrjoin AS R
    ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
    WHERE R.filter_key = 'rtarget'
    SETTINGS query_plan_use_new_logical_join_step = 1;
SELECT 'bilateral_right_where_count_new=0', count() FROM items_arrjoin AS L
    INNER JOIN right_arrjoin AS R
    ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
    WHERE R.filter_key = 'rtarget'
    SETTINGS query_plan_use_new_logical_join_step = 0;
SELECT 'bilateral_right_where_arrjoin_count_in_plan_new=1',
    countSubstrings(arrayStringConcat(groupArray(explain), char(10)),
                    'ARRAY JOIN ')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM items_arrjoin AS L
        INNER JOIN right_arrjoin AS R
        ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
        WHERE R.filter_key = 'rtarget'
        SETTINGS query_plan_use_new_logical_join_step = 1
);

-- (12) Bilateral, WHERE on BOTH sides (separable conjuncts) -- both
--      left- and right-side push-down trigger in the same query.
SELECT 'bilateral_both_where_count_new=1', count() FROM items_arrjoin AS L
    INNER JOIN right_arrjoin AS R
    ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
    WHERE L.filter_key = 'target' AND R.filter_key = 'rtarget'
    SETTINGS query_plan_use_new_logical_join_step = 1;
SELECT 'bilateral_both_where_count_new=0', count() FROM items_arrjoin AS L
    INNER JOIN right_arrjoin AS R
    ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
    WHERE L.filter_key = 'target' AND R.filter_key = 'rtarget'
    SETTINGS query_plan_use_new_logical_join_step = 0;
SELECT 'bilateral_both_where_arrjoin_count_in_plan_new=1',
    countSubstrings(arrayStringConcat(groupArray(explain), char(10)),
                    'ARRAY JOIN ')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM items_arrjoin AS L
        INNER JOIN right_arrjoin AS R
        ON arrayJoin(L.tag_ids) = arrayJoin(R.arr)
        WHERE L.filter_key = 'target' AND R.filter_key = 'rtarget'
        SETTINGS query_plan_use_new_logical_join_step = 1
);

DROP TABLE items_arrjoin;
DROP TABLE tags_arrjoin;
DROP TABLE right_arrjoin;
