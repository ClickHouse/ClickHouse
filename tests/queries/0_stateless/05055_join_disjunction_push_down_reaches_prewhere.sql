-- `tryPushDownFilter` extracts the per-side part of an `OR` across a join and inserts it as a new
-- filter two levels below itself, under the join. It reported a single layer of re-traversal, so the
-- new filter was never visited again: it stayed separated from the read by the expression that renames
-- the read's columns, and `optimizePrewhere` - which only moves a filter that sits directly on the
-- read - could not take it. The predicate was applied, just never as PREWHERE.

DROP TABLE IF EXISTS pw_disj_left;
DROP TABLE IF EXISTS pw_disj_right;

CREATE TABLE pw_disj_left (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO pw_disj_left SELECT number, if(number % 2 = 0, 'FRANCE', 'GERMANY') FROM numbers(1000);

CREATE TABLE pw_disj_right (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO pw_disj_right SELECT number, if(number % 3 = 0, 'FRANCE', 'GERMANY') FROM numbers(1000);

SET use_join_disjunctions_push_down = 1;
SET optimize_move_to_prewhere = 1;
-- The test runner randomizes this one, and with it off `optimizePrewhere` never runs at all.
SET query_plan_optimize_prewhere = 1;

-- One per side: each read gets the part of the `OR` that only mentions its own columns.
SELECT countIf(explain LIKE '%Prewhere filter column:%') AS pushed_to_prewhere
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM pw_disj_left AS l, pw_disj_right AS r
    WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'))
);

-- The predicate was always applied, so the answer never depended on this.
SELECT count() FROM pw_disj_left AS l, pw_disj_right AS r
WHERE l.k = r.k AND ((l.name = 'FRANCE' AND r.name = 'GERMANY') OR (l.name = 'GERMANY' AND r.name = 'FRANCE'));

DROP TABLE pw_disj_left;
DROP TABLE pw_disj_right;
