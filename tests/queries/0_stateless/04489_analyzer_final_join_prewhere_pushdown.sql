-- FINAL is a per-table modifier: `<t1> FINAL JOIN <t2>` must read only `t1` with FINAL, not `t2`.

DROP TABLE IF EXISTS t1_final_join;
DROP TABLE IF EXISTS t2_final_join;

CREATE TABLE t1_final_join (id Int32, s String) ENGINE = ReplacingMergeTree ORDER BY id;
CREATE TABLE t2_final_join (id Int32, v String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1_final_join VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t2_final_join VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- Exactly one table (t1) must be read with FINAL; before the fix t2 was read with FINAL too (count 2).

SET enable_analyzer = 1;
SELECT 'analyzer tables read with FINAL', countIf(explain LIKE '%FINAL: 1%')
FROM
(
    EXPLAIN actions = 1
    SELECT t2_final_join.v, t1_final_join.s
    FROM t1_final_join FINAL
    JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
    WHERE t2_final_join.v = 'x'
);

SET enable_analyzer = 0;
SELECT 'legacy tables read with FINAL', countIf(explain LIKE '%FINAL: 1%')
FROM
(
    EXPLAIN actions = 1
    SELECT t2_final_join.v, t1_final_join.s
    FROM t1_final_join FINAL
    JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
    WHERE t2_final_join.v = 'x'
);

-- Results must stay correct in both modes.
SET enable_analyzer = 1;
SELECT 'result analyzer', t2_final_join.v, t1_final_join.s
FROM t1_final_join FINAL
JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
WHERE t2_final_join.v = 'x';

SET enable_analyzer = 0;
SELECT 'result legacy', t2_final_join.v, t1_final_join.s
FROM t1_final_join FINAL
JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
WHERE t2_final_join.v = 'x';

DROP TABLE t1_final_join;
DROP TABLE t2_final_join;

-- FINAL is a per-table modifier: every FINAL/JOIN combination must read (and deduplicate) exactly the
-- tables the `FINAL` keyword is attached to, and only those. Both tables are ReplacingMergeTree with
-- un-merged duplicate parts; merges are stopped so the un-merged state (and thus the observable effect
-- of FINAL) is deterministic. Only enable_analyzer = 1 is checked here: the old planner mishandles
-- FINAL on the right (non-first) table, so it disagrees on the `right`/`both` shapes.

DROP TABLE IF EXISTS lhs_final;
DROP TABLE IF EXISTS rhs_final;

CREATE TABLE lhs_final (id Int32, s String, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY id;
CREATE TABLE rhs_final (id Int32, s String, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY id;

SYSTEM STOP MERGES lhs_final;
SYSTEM STOP MERGES rhs_final;

INSERT INTO lhs_final VALUES (1, 'l_old', 1);
INSERT INTO lhs_final VALUES (1, 'l_new', 2);
INSERT INTO rhs_final VALUES (1, 'r_old', 1);
INSERT INTO rhs_final VALUES (1, 'r_new', 2);

SET enable_analyzer = 1;

-- The number of tables planned as FINAL reads must equal the number of `FINAL` keywords.
SELECT 'final reads none',  countIf(explain LIKE '%FINAL: 1%') FROM (EXPLAIN actions = 1 SELECT lhs_final.s, rhs_final.s FROM lhs_final       JOIN rhs_final       ON rhs_final.id = lhs_final.id);
SELECT 'final reads left',  countIf(explain LIKE '%FINAL: 1%') FROM (EXPLAIN actions = 1 SELECT lhs_final.s, rhs_final.s FROM lhs_final FINAL JOIN rhs_final       ON rhs_final.id = lhs_final.id);
SELECT 'final reads right', countIf(explain LIKE '%FINAL: 1%') FROM (EXPLAIN actions = 1 SELECT lhs_final.s, rhs_final.s FROM lhs_final       JOIN rhs_final FINAL ON rhs_final.id = lhs_final.id);
SELECT 'final reads both',  countIf(explain LIKE '%FINAL: 1%') FROM (EXPLAIN actions = 1 SELECT lhs_final.s, rhs_final.s FROM lhs_final FINAL JOIN rhs_final FINAL ON rhs_final.id = lhs_final.id);

-- Results: FINAL deduplicates only the table(s) it is attached to.
SELECT 'result none',  arraySort(groupArray(lhs_final.s || '/' || rhs_final.s)) FROM lhs_final       JOIN rhs_final       ON rhs_final.id = lhs_final.id;
SELECT 'result left',  arraySort(groupArray(lhs_final.s || '/' || rhs_final.s)) FROM lhs_final FINAL JOIN rhs_final       ON rhs_final.id = lhs_final.id;
SELECT 'result right', arraySort(groupArray(lhs_final.s || '/' || rhs_final.s)) FROM lhs_final       JOIN rhs_final FINAL ON rhs_final.id = lhs_final.id;
SELECT 'result both',  arraySort(groupArray(lhs_final.s || '/' || rhs_final.s)) FROM lhs_final FINAL JOIN rhs_final FINAL ON rhs_final.id = lhs_final.id;

DROP TABLE lhs_final;
DROP TABLE rhs_final;
