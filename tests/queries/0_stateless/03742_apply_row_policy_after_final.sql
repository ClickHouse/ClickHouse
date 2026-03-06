-- Tags: no-parallel
-- Test for apply_row_policy_after_final setting with ReplacingMergeTree, https://github.com/ClickHouse/ClickHouse/issues/90986

DROP TABLE IF EXISTS tab;
DROP ROW POLICY IF EXISTS pol1 ON tab;

CREATE TABLE tab (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab VALUES (1, 'ccc', 2);

SELECT '= no row policy =';
SET apply_row_policy_after_final = 0;
SELECT '--- raw';
SELECT * FROM tab ORDER BY x, version;
SELECT '--- final';
SELECT * FROM tab FINAL ORDER BY x;

-- Create row policy that filters out rows where y = 'ccc'
CREATE ROW POLICY pol1 ON tab USING y != 'ccc' TO ALL;

SELECT '= with row policy (default behavior - filter before FINAL) =';
SELECT '--- raw';
SELECT * FROM tab ORDER BY x, version;
SELECT '--- final';
SELECT * FROM tab FINAL ORDER BY x;

SELECT '= with row policy and apply_row_policy_after_final =';
SET apply_row_policy_after_final = 1;
SELECT '--- raw';
SELECT * FROM tab ORDER BY x, version;
SELECT '--- final';
-- now the row with x=1, y='ccc', version=2 should be selected by FINAL first,
-- then filtered out by the row policy, leaving only x=2
SELECT * FROM tab FINAL ORDER BY x;

DROP ROW POLICY pol1 ON tab;
DROP TABLE tab;

-- Test that filter on ORDER BY column is still applied before FINAL (optimization)
SELECT '';
SELECT '= row policy on ORDER BY column should be applied before FINAL =';

DROP TABLE IF EXISTS tab2;
DROP ROW POLICY IF EXISTS pol2 ON tab2;

CREATE TABLE tab2 (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab2 VALUES (1, 'aaa', 1), (2, 'bbb', 1), (3, 'ccc', 1);
INSERT INTO tab2 VALUES (1, 'ddd', 2);

-- row policy on x (ORDER BY column) - should be applied before FINAL
CREATE ROW POLICY pol2 ON tab2 USING x != 1 TO ALL;

SET apply_row_policy_after_final = 1;
SELECT '--- final with policy on ORDER BY column';
-- both rows with x=1 should be filtered out before FINAL
SELECT * FROM tab2 FINAL ORDER BY x;

DROP ROW POLICY pol2 ON tab2;
DROP TABLE tab2;

-- test that PREWHERE is also deferred when row policy is deferred
SELECT '';
SELECT '= PREWHERE should be deferred together with row policy =';

DROP TABLE IF EXISTS tab3;
DROP ROW POLICY IF EXISTS pol3 ON tab3;

CREATE TABLE tab3 (x UInt32, y String, z UInt32, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab3 VALUES (1, 'aaa', 100, 1), (2, 'bbb', 200, 1);
INSERT INTO tab3 VALUES (1, 'ccc', 300, 2), (2, 'ddd', 300, 2);

CREATE ROW POLICY pol3 ON tab3 USING y != 'ccc' TO ALL;

SELECT '--- without setting';
SET apply_row_policy_after_final = 0;

-- PREWHERE z < 250 filters (1, 'ccc', 300, 2) and (2, 'ddd', 300, 2)
-- FINAL gets (1, 'aaa', 100, 1) and (2, 'bbb', 200, 1)
SELECT * FROM tab3 FINAL PREWHERE z < 250 ORDER BY x;

SELECT '--- with setting';
SET apply_row_policy_after_final = 1;
-- FINAL first: (1, 'ccc', 300, 2) and (2, 'ddd', 300, 2)
-- row policy removes (1, 'ccc', 300, 2)
-- PREWHERE z < 250: (2, 'ddd', 300, 2) does not pass, so nothing
SELECT * FROM tab3 FINAL PREWHERE z < 250 ORDER BY x;

DROP ROW POLICY pol3 ON tab3;
DROP TABLE tab3;

-- Test that SELECT with subset of columns works when row policy uses other columns
SELECT '';
SELECT '= SELECT subset of columns with row policy on other column =';

DROP TABLE IF EXISTS tab4;
DROP ROW POLICY IF EXISTS pol4 ON tab4;

CREATE TABLE tab4 (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab4 VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab4 VALUES (1, 'ccc', 2);

-- Row policy on y, but we SELECT only x
CREATE ROW POLICY pol4 ON tab4 USING y != 'ccc' TO ALL;

SET apply_row_policy_after_final = 1;
SELECT '--- SELECT x only (row policy on y should still work)';
SELECT x FROM tab4 FINAL ORDER BY x;

SELECT '--- SELECT x, version (row policy on y should still work)';
SELECT x, version FROM tab4 FINAL ORDER BY x;

DROP ROW POLICY pol4 ON tab4;
DROP TABLE tab4;

DROP TABLE IF EXISTS tab_final;
DROP ROW POLICY IF EXISTS pol_final ON tab_final;

CREATE TABLE tab_final (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab_final VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_final VALUES (1, 'ccc', 2);

SET apply_prewhere_after_final = 1;
SELECT '--- PREWHERE after FINAL';
SELECT x FROM tab_final FINAL PREWHERE y != 'ccc' ORDER BY x;

SET apply_prewhere_after_final = 0;
SELECT '--- PREWHERE before FINAL';
SELECT x FROM tab_final FINAL PREWHERE y != 'ccc' ORDER BY x;

DROP TABLE tab_final;

-- test that partition pruning works OK with row policy
-- When row policy column in the partition key, minmax index must not prune partitions that FINAL needs for correct deduplication
SELECT '';

DROP TABLE IF EXISTS tab_part;
DROP ROW POLICY IF EXISTS pol_part ON tab_part;

CREATE TABLE tab_part (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) PARTITION BY y ORDER BY x;

INSERT INTO tab_part VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_part VALUES (1, 'ccc', 2);

CREATE ROW POLICY pol_part ON tab_part USING y != 'ccc' TO ALL;

SET apply_row_policy_after_final = 0;
SELECT '--- filter before FINAL';
-- partition 'ccc' pruned, FINAL sees (1,'aaa',1) and (2,'bbb',1)
SELECT * FROM tab_part FINAL ORDER BY x;

SET apply_row_policy_after_final = 1;
SELECT '--- deferred';
-- all partitions read, FINAL picks (1,'ccc',2) then row policy removes it
SELECT * FROM tab_part FINAL ORDER BY x;

DROP ROW POLICY pol_part ON tab_part;
DROP TABLE tab_part;
