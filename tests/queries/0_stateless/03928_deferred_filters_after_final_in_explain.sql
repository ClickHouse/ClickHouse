-- Tags: no-parallel
-- test that EXPLAIN shows deferred filter information for apply_prewhere_after_final / apply_row_policy_after_final

DROP TABLE IF EXISTS tab;
DROP ROW POLICY IF EXISTS pol1 ON tab;

CREATE TABLE tab (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab SELECT 1, 'aaa', 1;
INSERT INTO tab SELECT 2, 'bbb', 1;
INSERT INTO tab SELECT 1, 'ccc', 2;

CREATE ROW POLICY pol1 ON tab USING y != 'ccc' TO ALL;

SET enable_analyzer = 1;

SELECT '= full plan: both deferred =';
EXPLAIN actions=1 SELECT * FROM tab FINAL PREWHERE y != 'ccc' ORDER BY x
SETTINGS apply_row_policy_after_final=1, apply_prewhere_after_final=1, optimize_read_in_order=1;

SELECT '= full plan: nothing deferred =';
EXPLAIN actions=1 SELECT * FROM tab FINAL PREWHERE y != 'ccc' ORDER BY x
SETTINGS apply_row_policy_after_final=0, apply_prewhere_after_final=0, optimize_read_in_order=1;

SELECT '= row policy deferred =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab FINAL ORDER BY x SETTINGS apply_row_policy_after_final=1, apply_prewhere_after_final=0) WHERE explain LIKE '%Deferred%';

SELECT '= row policy not deferred =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab FINAL ORDER BY x SETTINGS apply_row_policy_after_final=0, apply_prewhere_after_final=0) WHERE explain LIKE '%Deferred%';

SELECT '= prewhere deferred =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab FINAL PREWHERE y != 'ccc' ORDER BY x SETTINGS apply_prewhere_after_final=1, apply_row_policy_after_final=0) WHERE explain LIKE '%Deferred%';

SELECT '= prewhere not deferred =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab FINAL PREWHERE y != 'ccc' ORDER BY x SETTINGS apply_prewhere_after_final=0, apply_row_policy_after_final=0) WHERE explain LIKE '%Deferred%';

SELECT '= both deferred =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab FINAL PREWHERE y != 'ccc' ORDER BY x SETTINGS apply_row_policy_after_final=1, apply_prewhere_after_final=1) WHERE explain LIKE '%Deferred%';

SELECT '= row policy on non-sorting-key defers prewhere =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab FINAL PREWHERE y != 'ccc' ORDER BY x SETTINGS apply_row_policy_after_final=1, apply_prewhere_after_final=0) WHERE explain LIKE '%Deferred%';

SELECT '= no FINAL - no deferred =';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab ORDER BY x SETTINGS apply_row_policy_after_final=1, apply_prewhere_after_final=0) WHERE explain LIKE '%Deferred%';

DROP ROW POLICY pol1 ON tab;
DROP TABLE tab;
