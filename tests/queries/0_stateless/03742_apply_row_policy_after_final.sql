-- Tags: no-parallel
-- Test for apply_row_policy_after_final setting with ReplacingMergeTree, https://github.com/ClickHouse/ClickHouse/issues/90986

DROP TABLE IF EXISTS tab;
DROP ROW POLICY IF EXISTS pol1 ON tab;

CREATE TABLE tab (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab VALUES (1, 'ccc', 2);

SELECT '= no row policy =';
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
